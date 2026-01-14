/*
 * Copyright 2025 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.smb

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SideInput, SideInputContext}
import org.apache.beam.sdk.extensions.smb.{
  BucketMetadata,
  BucketShardId,
  SmbValidationHelper,
  SortedBucketIO,
  SortedBucketSource,
  TargetParallelism
}
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver
import org.apache.beam.sdk.values.{KV, TupleTag}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Implementation logic for SMBCollection. */
private[smb] object SMBCollectionImpl {

  /** Serializable info for writing output files - stores FileOperations directly */
  private[smb] case class OutputInfo(
    fileOperations: org.apache.beam.sdk.extensions.smb.FileOperations[_],
    outputDirectory: String,
    filenamePrefix: String,
    filenameSuffix: String
  ) extends Serializable

  /** Extract serializable output info from a TransformOutput */
  private[smb] def extractOutputInfo(
    output: SortedBucketIO.TransformOutput[_, _, _]
  ): OutputInfo = {
    import org.apache.beam.sdk.extensions.smb.SMBCollectionHelper
    OutputInfo(
      SMBCollectionHelper.getFileOperations(output),
      SMBCollectionHelper.getOutputDirectory(output).toString,
      SMBCollectionHelper.getFilenamePrefix(output),
      SMBCollectionHelper.getFilenameSuffix(output)
    )
  }

  // Explicit fail-fast coder for types that should never be serialized (e.g. CoGbkResult)
  private class FailCoder[T](typeName: String, reason: String)
      extends org.apache.beam.sdk.coders.Coder[T] {

    override def encode(value: T, outStream: java.io.OutputStream): Unit = {
      throw new UnsupportedOperationException(
        s"$typeName should never be serialized - $reason. " +
          "This is likely a bug in SMBCollection implementation."
      )
    }

    override def decode(inStream: java.io.InputStream): T = {
      throw new UnsupportedOperationException(
        s"$typeName should never be deserialized - $reason. " +
          "This is likely a bug in SMBCollection implementation."
      )
    }

    override def getCoderArguments: java.util.List[_ <: org.apache.beam.sdk.coders.Coder[_]] =
      java.util.Collections.emptyList()

    override def verifyDeterministic(): Unit = ()

    // Beam compatibility: mark as deterministic to avoid pipeline validation warnings
    override def isRegisterByteSizeObserverCheap(value: T): Boolean = true
  }

  def validateAndGetMetadata(
    sources: List[SortedBucketIO.Read[_]]
  ): Option[BucketMetadata[_, _, _]] = {
    val bucketedInputs = sources.map(_.toBucketedInput(SortedBucketSource.Keying.PRIMARY))
    SmbValidationHelper.validateSources(bucketedInputs.asJava) // Validates hash type, bucket counts
    Option(SmbValidationHelper.getFirstMetadata(bucketedInputs.asJava))
  }

  // Validates that all sources have compatible key classes
  def validateKeyClasses[K](
    sources: List[SortedBucketIO.Read[_]],
    expectedKeyClass: Class[_],
    expectedKeyClassSecondary: Option[Class[_]] = None
  ): Unit = {
    val bucketedInputs = sources.map(_.toBucketedInput(SortedBucketSource.Keying.PRIMARY))
    bucketedInputs.foreach { input =>
      input.getSourceMetadata.mapping.values.forEach { sm =>
        val metadata = sm.metadata

        // Validate primary key class
        require(
          SmbValidationHelper.keyClassMatches(metadata, expectedKeyClass),
          s"Source has primary key class ${metadata.getKeyClass} but expected $expectedKeyClass"
        )

        // Validate secondary key class if provided
        expectedKeyClassSecondary.foreach { expectedSecondary =>
          require(
            SmbValidationHelper.keyClassSecondaryMatches(metadata, expectedSecondary),
            s"Source has secondary key class ${metadata.getKeyClassSecondary} but expected $expectedSecondary"
          )
        }
      }
    }
  }

  // Root always works with List[Iterable[Any]] - user transformations applied as child nodes
  // K1, K2 tracked in type for saveAsSortedBucket type safety
  def createRoot[K1: Coder, K2: Coder, V: Coder](
    sources: List[SortedBucketIO.Read[_]],
    targetParallelism: TargetParallelism,
    fromResult: (Any, Any, List[Iterable[Any]]) => V, // Receives (key1, key2, values)
    keyClass: Class[K1],
    keyClassSecondary: Class[K2]
  )(implicit sc: ScioContext): SMBCollection[K1, K2, V] = {
    validateAndGetMetadata(sources)

    val rootContext =
      new SMBRootContext[K1, K2](sources, targetParallelism, keyClass, keyClassSecondary)(sc)

    // Fail-fast coder to catch serialization bugs early
    implicit val inOutCoder: Coder[List[Iterable[Any]]] =
      Coder.beam(
        new FailCoder[List[Iterable[Any]]](
          "List[Iterable[Any]]",
          "it only exists during SMB execution"
        )
      )

    val core = new SMBCollectionCore[K1, K2, List[Iterable[Any]], List[Iterable[Any]]](
      transformFn = None, // Root has no transformation - just fans out to children
      rootContext = rootContext
    )

    rootContext.setCore(core)

    sc.onClose { _ =>
      rootContext.sealAndExecute()
    }

    // Create a transformation that maps raw key groups to user's V type
    // This transformation has access to keys via the core's internal API
    core.transform[V] { (_: SideInputContext[_], k1: K1, k2: K2, values: List[Iterable[Any]]) =>
      Some(fromResult(k1.asInstanceOf[Any], k2.asInstanceOf[Any], values))
    }
  }

  class SortedBucketMultiTransformDoFn[K1, K2](
    bucketedInputs: java.util.List[SortedBucketSource.BucketedInput[_]], // Serializable!
    keyFn: SortedBucketIO.ComparableKeyBytes => (K1, K2),
    keyComparator: java.util.Comparator[SortedBucketIO.ComparableKeyBytes],
    rootConsumer: SMBConsumer[K1, K2, List[Iterable[Any]]],
    sideInputs: Seq[SideInput[_]],
    metricsPrefix: String
  ) extends org.apache.beam.sdk.transforms.DoFn[
        org.apache.beam.sdk.extensions.smb.SortedBucketTransform.BucketItem,
        Void // Unused main output
      ] {

    // Metrics for progress tracking
    private val keyGroupsProcessed: org.apache.beam.sdk.metrics.Counter =
      org.apache.beam.sdk.metrics.Metrics
        .counter(metricsPrefix, s"$metricsPrefix-KeyGroupsProcessed")
    private val elementsReadPerSource: List[org.apache.beam.sdk.metrics.Counter] =
      bucketedInputs.asScala.indices.map { idx =>
        org.apache.beam.sdk.metrics.Metrics
          .counter(metricsPrefix, s"$metricsPrefix-ElementsRead-Source-$idx")
      }.toList

    @org.apache.beam.sdk.transforms.DoFn.ProcessElement
    def processElement(
      @org.apache.beam.sdk.transforms.DoFn.Element element: org.apache.beam.sdk.extensions.smb.SortedBucketTransform.BucketItem,
      out: MultiOutputReceiver,
      c: ProcessContext,
      window: org.apache.beam.sdk.transforms.windowing.BoundedWindow
    ): Unit = {
      import org.apache.beam.sdk.extensions.smb.SMBCollectionHelper
      val bucketId = SMBCollectionHelper.getBucketOffsetId(element)
      val effectiveParallelism = SMBCollectionHelper.getEffectiveParallelism(element)

      rootConsumer.init(bucketId, effectiveParallelism)

      val sideInputContext = if (sideInputs.nonEmpty) {
        val processContext = c.asInstanceOf[org.apache.beam.sdk.transforms.DoFn[
          org.apache.beam.sdk.extensions.smb.SortedBucketTransform.BucketItem,
          AnyRef
        ]#ProcessContext]
        new SideInputContext[org.apache.beam.sdk.extensions.smb.SortedBucketTransform.BucketItem](
          processContext,
          window
        )
      } else {
        null.asInstanceOf[SideInputContext[_]]
      }

      rootConsumer.setSideInputContext(sideInputContext)

      val reader = new SimpleKeyGroupReader[(K1, K2)](
        bucketedInputs.asScala.toList,
        keyFn,
        keyComparator,
        metricsPrefix,
        bucketId,
        effectiveParallelism,
        c.getPipelineOptions
      )

      var keyGroup = reader.readNext()
      var keyGroupCount = 0L
      while (keyGroup != null) {
        val (key1, key2) = keyGroup.getKey
        val valuesBySource = keyGroup.getValue // List[Iterable[Any]] - lazy iterables

        rootConsumer.accept(key1, key2, valuesBySource, out)

        // CRITICAL: Exhaust all iterables before next iteration
        // This ensures:
        // 1. Unconsumed iterables don't block reader advancement
        // 2. Partially consumed iterables are fully consumed
        // 3. SourceIterator can advance to next key group
        // Also collect element counts from ExhaustableLazyIterables
        valuesBySource.zipWithIndex.foreach { case (iterable, idx) =>
          iterable match {
            case exhaustable: ExhaustableLazyIterable[_] =>
              exhaustable.exhaust()
              val count = exhaustable.getElementCount
              if (count > 0) {
                elementsReadPerSource(idx).inc(count)
              }
            case _ => // Other Iterable types don't need exhaustion
          }
        }

        // Update metrics: count key groups processed
        keyGroupsProcessed.inc()
        keyGroupCount += 1

        keyGroup = reader.readNext()
      }

      // Finish all consumers after processing all key groups
      // Consumers output bucket files to their tags for RenameBuckets transform
      rootConsumer.finish(out)
    }
  }

  def executeGraph[K1, K2](
    sources: List[SortedBucketIO.Read[_]],
    targetParallelism: TargetParallelism,
    root: SMBCollectionCore[K1, K2, List[Iterable[Any]], List[Iterable[Any]]],
    sideInputs: Seq[SideInput[_]],
    keyClassSecondary: Class[K2]
  )(implicit sc: ScioContext): SMBExecutionResult = {
    val metricsPrefix = "SMBCollection"
    val outputNodes = mutable.Buffer.empty[SMBToSCollectionOutput[K1, K2, _]]
    val fileOutputConsumers = mutable.Buffer
      .empty[(SMBSaveAsSortedBucketOutput[K1, K2, _], SMBFileOutputConsumer[K1, K2, _], TupleTag[KV[BucketShardId, ResourceId]])]

    val childConsumers: Seq[SMBConsumer[K1, K2, List[Iterable[Any]]]] = root.children.map { child =>
      child.buildConsumer(outputNodes, fileOutputConsumers, metricsPrefix)
    }.toSeq

    // Root just fans out to children without transformation (identity transform)
    val rootConsumer: SMBConsumer[K1, K2, List[Iterable[Any]]] =
      new PassThroughConsumer[K1, K2, List[Iterable[Any]]](
        children = childConsumers
      )

    // Extract bucket file tags from file output consumers
    val bucketFileTags = fileOutputConsumers.map(_._3).toSeq

    val pCollectionTuple = createSMBTransform(
      sources,
      root.key1Coder,
      root.key2Coder,
      targetParallelism,
      rootConsumer,
      sideInputs,
      outputNodes.toSeq,
      bucketFileTags,
      keyClassSecondary,
      metricsPrefix
    )

    // Populate deferred taps - taps read files directly from final location
    root.rootContext.deferredTaps.foreach { deferredTap =>
      // Use helper to preserve types while working with existential DeferredTap[_]
      def populateTap[V](dt: DeferredTap[V]): Unit = {
        // Files are written directly to final location, so tap can read them immediately
        // Pass output info to SmbIO.tap - FileOperations is already available
        val tap = SmbIO.tap(dt.outputInfo)(dt.valueCoder).apply(sc)
        val closedTap = com.spotify.scio.io.ClosedTap(tap)
        dt.setTap(closedTap)
      }
      // Call helper with existential type - the helper recaptures the type parameter
      populateTap(deferredTap)
    }

    SMBExecutionResult(
      pCollectionTuple = if (outputNodes.nonEmpty) Some(pCollectionTuple) else None
    )
  }

  def createSMBTransform[K1, K2](
    sources: List[SortedBucketIO.Read[_]],
    key1Coder: Coder[K1],
    key2Coder: Coder[K2],
    targetParallelism: TargetParallelism,
    rootConsumer: SMBConsumer[K1, K2, List[Iterable[Any]]],
    sideInputs: Seq[SideInput[_]],
    outputNodes: Seq[SMBToSCollectionOutput[K1, K2, _]],
    bucketFileTags: Seq[TupleTag[KV[BucketShardId, ResourceId]]],
    keyClassSecondary: Class[K2],
    metricsPrefix: String = "SMBCollection"
  )(implicit sc: ScioContext): org.apache.beam.sdk.values.PCollectionTuple = {
    val isPrimaryOnly = keyClassSecondary == classOf[Void]

    val keying = if (isPrimaryOnly) {
      SortedBucketSource.Keying.PRIMARY
    } else {
      SortedBucketSource.Keying.PRIMARY_AND_SECONDARY
    }

    // Convert sources to BucketedInput using polymorphism - no instanceof checks needed!
    val bucketedInputs = sources.map(_.toBucketedInput(keying)).asJava

    import org.apache.beam.sdk.extensions.smb.SMBCollectionHelper
    val sourceSpec = SMBCollectionHelper.createSourceSpec(bucketedInputs)

    // Use KV[K1, K2] for Beam compatibility
    val bucketSource = SMBCollectionHelper.createBucketSource[KV[K1, K2]](
      bucketedInputs,
      targetParallelism,
      1, // numShards
      0, // bucketOffset
      sourceSpec,
      -1 // keyCacheSize (use default)
    )

    val buckets =
      sc.pipeline.apply("SMB-Read-Buckets", org.apache.beam.sdk.io.Read.from(bucketSource))

    val someMetadata = bucketedInputs.asScala.head.getSourceMetadata.mapping.values.iterator.next.metadata

    // Use java.util.function.Function to avoid referencing package-private KeyFn type
    val keyFnAndComparator = if (isPrimaryOnly) {
      // Primary key only: use primary key function and comparator
      val primaryKeyCoder = SMBCollectionHelper.getPrimaryKeyCoder(someMetadata)
      val fn: java.util.function.Function[SortedBucketIO.ComparableKeyBytes, _] =
        SortedBucketIO.ComparableKeyBytes.keyFnPrimary(primaryKeyCoder)
      val comp = new SortedBucketIO.PrimaryKeyComparator()
      (fn, comp)
    } else {
      // Secondary key: use composite key function and comparator
      val primaryKeyCoder = SMBCollectionHelper.getPrimaryKeyCoder(someMetadata)
      val secondaryKeyCoder = SMBCollectionHelper.getSecondaryKeyCoder(someMetadata)
      val fn: java.util.function.Function[SortedBucketIO.ComparableKeyBytes, _] =
        SortedBucketIO.ComparableKeyBytes.keyFnPrimaryAndSecondary(
          primaryKeyCoder,
          secondaryKeyCoder
        )
      val comp = new SortedBucketIO.PrimaryAndSecondaryKeyComparator()
      (fn, comp)
    }
    val javaKeyFn = keyFnAndComparator._1
    val keyComparator = keyFnAndComparator._2

    // Convert Beam KV to Scala tuple for consumer
    val keyFn: SortedBucketIO.ComparableKeyBytes => (K1, K2) = { kb =>
      val beamKv = javaKeyFn.apply(kb)
      if (isPrimaryOnly) {
        // Primary only - beamKv is just K1, pair with Void
        (beamKv.asInstanceOf[K1], null.asInstanceOf[K2])
      } else {
        // Composite - beamKv is KV[K1, K2]
        val kv = beamKv.asInstanceOf[KV[K1, K2]]
        (kv.getKey, kv.getValue)
      }
    }

    // Main tag for unused main output (Void output)
    val mainTag = new TupleTag[Void]("main-unused")

    val doFn = new SortedBucketMultiTransformDoFn[K1, K2](
      bucketedInputs, // BucketedInput is designed to be serializable
      keyFn,
      keyComparator,
      rootConsumer,
      sideInputs,
      metricsPrefix
    )

    // Include both PCollection output tags and bucket file tags
    val sideTagList = (outputNodes.map(_.tag.asInstanceOf[TupleTag[_]]) ++ bucketFileTags)
      .foldLeft(org.apache.beam.sdk.values.TupleTagList.empty) { (list, tag) =>
        list.and(tag)
      }

    val parDo = org.apache.beam.sdk.transforms.ParDo
      .of(doFn)
      .withOutputTags(mainTag, sideTagList)

    val parDoWithSides = if (sideInputs.nonEmpty) {
      parDo.withSideInputs(sideInputs.map(_.view).asJava)
    } else {
      parDo
    }

    val result = buckets.apply("SMB-Transform", parDoWithSides)

    // Create Beam coders
    val beamKey1Coder = com.spotify.scio.coders.CoderMaterializer.beam(sc, key1Coder)
    val beamKey2Coder = com.spotify.scio.coders.CoderMaterializer.beam(sc, key2Coder)

    // Set coder for main tag (unused Void output)
    val mainPc = result.get(mainTag)
    mainPc.setCoder(org.apache.beam.sdk.coders.VoidCoder.of())

    outputNodes.foreach { node =>
      // Get value coder - outputNodes only contains toSCollection nodes, use CoderMaterializer
      val valueCoder = node.valueCoder.asInstanceOf[Coder[Any]]
      val beamValueCoder = com.spotify.scio.coders.CoderMaterializer.beam(sc, valueCoder)

      // Create beam key coder for output nodes - use KV[K1, K2] for Beam
      val beamKeyCoder = if (isPrimaryOnly) {
        beamKey1Coder
      } else {
        org.apache.beam.sdk.coders.KvCoder.of(beamKey1Coder, beamKey2Coder)
      }

      val kvCoder = org.apache.beam.sdk.coders.KvCoder.of(beamKeyCoder, beamValueCoder)
      // Use double cast to bypass invariant generics type checking
      result
        .get(node.tag)
        .asInstanceOf[org.apache.beam.sdk.values.PCollection[Any]]
        .setCoder(kvCoder.asInstanceOf[org.apache.beam.sdk.coders.Coder[Any]])
    }

    // Set coders for bucket file PCollections (KV[BucketShardId, ResourceId])
    bucketFileTags.foreach { tag =>
      val bucketFilesPc = result.get(tag)
      bucketFilesPc.setCoder(
        org.apache.beam.sdk.coders.KvCoder.of(
          org.apache.beam.sdk.extensions.smb.BucketShardId.BucketShardIdCoder.of(),
          org.apache.beam.sdk.io.fs.ResourceIdCoder.of()
        )
      )
    }

    result
  }
}
