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

  /** Serializable metadata for reconstructing SortedBucketIO.Read in @Setup */
  private[smb] case class SourceMetadata(
    tupleTag: org.apache.beam.sdk.values.TupleTag[_],
    schema: org.apache.avro.Schema,
    inputDirectories: java.util.List[String],
    isParquet: Boolean // Track whether this is Parquet or Avro
  ) extends Serializable

  /** Serializable metadata for reconstructing FileOperations in tap */
  private[smb] case class OutputMetadata(
    schema: org.apache.avro.Schema,
    outputDirectory: String,
    filenamePrefix: String,
    filenameSuffix: String,
    isParquet: Boolean // Track whether this is Parquet or Avro
  ) extends Serializable

  /** Extract serializable metadata from a SortedBucketIO.Read */
  private[smb] def extractSourceMetadata(read: SortedBucketIO.Read[_]): SourceMetadata = {
    import org.apache.beam.sdk.extensions.smb.SMBCollectionHelper
    val tupleTag = SMBCollectionHelper.getTupleTag(read)
    val schema = SMBCollectionHelper.getSchema(read)
    val inputDirectories = SMBCollectionHelper.getInputDirectories(read)
    val isParquet =
      read.isInstanceOf[org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO.Read[_]]
    SourceMetadata(tupleTag, schema, inputDirectories, isParquet)
  }

  /** Extract serializable metadata from a TransformOutput */
  private[smb] def extractOutputMetadata(
    output: SortedBucketIO.TransformOutput[_, _, _]
  ): OutputMetadata = {
    import org.apache.beam.sdk.extensions.smb.SMBCollectionHelper
    val fileOps = SMBCollectionHelper.getFileOperations(output)
    val avroCoder =
      fileOps.getCoder().asInstanceOf[org.apache.beam.sdk.extensions.avro.coders.AvroCoder[_]]
    val schema = avroCoder.getSchema
    val outputDir = SMBCollectionHelper.getOutputDirectory(output).toString
    val prefix = SMBCollectionHelper.getFilenamePrefix(output)
    val suffix = SMBCollectionHelper.getFilenameSuffix(output)
    val isParquet = output.isInstanceOf[
      org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO.TransformOutput[_, _, _]
    ]
    OutputMetadata(schema, outputDir, prefix, suffix, isParquet)
  }

  /** Reconstruct SortedBucketIO.Read from metadata */
  private[smb] def reconstructRead(meta: SourceMetadata): SortedBucketIO.Read[_] = {
    // Reconstruct fresh Read with new FileOperations (preserves DatumFactory)
    // Get the record class from the schema
    // TODO: Consider caching schema→class lookups if profiling shows significant overhead.
    //       Currently happens once per worker in @Setup, so likely not a bottleneck.
    val specificData = new org.apache.avro.specific.SpecificData()
    val recordClass = Option(specificData.getClass(meta.schema))
      .getOrElse(
        throw new IllegalStateException(
          s"Cannot resolve SpecificRecord class for schema '${meta.schema.getFullName}'. " +
            s"Ensure the Avro schema has 'namespace' and 'name' properties and the corresponding " +
            s"class is available on the classpath."
        )
      )
      .asInstanceOf[Class[org.apache.avro.specific.SpecificRecord]]
    val tupleTag = meta.tupleTag
      .asInstanceOf[org.apache.beam.sdk.values.TupleTag[org.apache.avro.specific.SpecificRecord]]

    if (meta.isParquet) {
      // Reconstruct ParquetAvroSortedBucketIO.Read
      org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO
        .read(tupleTag, recordClass)
        .from(meta.inputDirectories)
    } else {
      // Reconstruct AvroSortedBucketIO.Read
      org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
        .read(tupleTag, recordClass)
        .from(meta.inputDirectories)
    }
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
  def createRoot[K1: Coder, K2: Coder, V: Coder](
    sources: List[SortedBucketIO.Read[_]],
    targetParallelism: TargetParallelism,
    fromResult: List[Iterable[Any]] => V,
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

    core.keyed.mapValues(fromResult)
  }

  class SortedBucketMultiTransformDoFn[K1, K2](
    sourceMetadata: List[SourceMetadata], // Serializable metadata only, no Beam objects!
    keying: SortedBucketSource.Keying,
    keyFn: SortedBucketIO.ComparableKeyBytes => (K1, K2),
    keyComparator: java.util.Comparator[SortedBucketIO.ComparableKeyBytes],
    rootConsumer: SMBConsumer[K1, K2, List[Iterable[Any]]],
    sideInputs: Seq[SideInput[_]],
    metricsPrefix: String,
    fileOutputConsumersWithTags: Seq[
      (TupleTag[KV[BucketShardId, ResourceId]], SMBFileOutputConsumer[K1, K2, _])
    ]
  ) extends org.apache.beam.sdk.transforms.DoFn[
        org.apache.beam.sdk.extensions.smb.SortedBucketTransform.BucketItem,
        Void // Unused main output
      ] {

    // Reconstruct everything fresh in @Setup to avoid serializing Beam objects (which lose DatumFactory)
    @transient private var sources: List[SortedBucketIO.Read[_]] = _
    @transient private var bucketedInputs: java.util.List[SortedBucketSource.BucketedInput[_]] = _

    // Metrics for progress tracking
    private val keyGroupsProcessed: org.apache.beam.sdk.metrics.Counter =
      org.apache.beam.sdk.metrics.Metrics
        .counter(metricsPrefix, s"$metricsPrefix-KeyGroupsProcessed")
    private val elementsReadPerSource: List[org.apache.beam.sdk.metrics.Counter] =
      sourceMetadata.indices.map { idx =>
        org.apache.beam.sdk.metrics.Metrics
          .counter(metricsPrefix, s"$metricsPrefix-ElementsRead-Source-$idx")
      }.toList

    @org.apache.beam.sdk.transforms.DoFn.Setup
    def setup(): Unit = {
      // Suppress unused warning - fileOutputConsumersWithTags not needed since we write files directly
      val _ = fileOutputConsumersWithTags
      // Reconstruct Read objects fresh - this creates new FileOperations with DatumFactory
      sources = sourceMetadata.map(reconstructRead)
      bucketedInputs = sources.map(_.toBucketedInput(keying)).asJava
    }

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
      // Consumer.finish() writes files AND metadata.json directly to final location
      rootConsumer.finish()

      // Skip outputting bucket file metadata - files are already in final location
      // and metadata.json is written by consumer.finish()
      // No finalization needed!
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
      .empty[(SMBSaveAsSortedBucketOutput[K1, K2, _], SMBFileOutputConsumer[K1, K2, _])]

    val childConsumers: Seq[SMBConsumer[K1, K2, List[Iterable[Any]]]] = root.children.map { child =>
      child.buildConsumer(outputNodes, fileOutputConsumers, metricsPrefix)
    }.toSeq

    // Root just fans out to children without transformation (identity transform)
    val rootConsumer: SMBConsumer[K1, K2, List[Iterable[Any]]] =
      new PassThroughConsumer[K1, K2, List[Iterable[Any]]](
        children = childConsumers
      )

    val bucketFileTags
      : Map[SMBSaveAsSortedBucketOutput[_, _, _], TupleTag[KV[BucketShardId, ResourceId]]] =
      fileOutputConsumers.map { case (fileOutput, _) =>
        (
          fileOutput,
          new TupleTag[KV[BucketShardId, ResourceId]](
            s"bucket-files-${System.identityHashCode(fileOutput)}"
          )
        )
      }.toMap

    // Pair consumers with tags to avoid capturing SMBSaveAsSortedBucketOutput in DoFn
    val fileOutputConsumersWithTags = fileOutputConsumers.map { case (fileOutput, consumer) =>
      (bucketFileTags(fileOutput), consumer)
    }.toSeq

    val pCollectionTuple = createSMBTransform(
      sources,
      root.key1Coder,
      root.key2Coder,
      targetParallelism,
      rootConsumer,
      sideInputs,
      outputNodes.toSeq,
      fileOutputConsumersWithTags,
      keyClassSecondary,
      metricsPrefix
    )

    val writeResults: Map[
      SMBSaveAsSortedBucketOutput[_, _, _],
      org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult
    ] =
      fileOutputConsumers.map { case (fileOutput, _) =>
        val bucketFilesTag = bucketFileTags(fileOutput)
        val bucketFiles = pCollectionTuple
          .get(bucketFilesTag)
          .setCoder(org.apache.beam.sdk.extensions.smb.SMBCollectionHelper.getBucketFilesCoder())

        val writeResult = applyFinalization(fileOutput, bucketFiles)(sc)

        (fileOutput.asInstanceOf[SMBSaveAsSortedBucketOutput[_, _, _]], writeResult)
      }.toMap

    // Populate deferred taps - create taps from metadata (no WriteResult needed)
    root.rootContext.deferredTaps.foreach { deferredTap =>
      // Use helper to preserve types while working with existential DeferredTap[_]
      def populateTap[V](dt: DeferredTap[V]): Unit = {
        // Files are written directly to final location, so tap can read them immediately
        // Pass output metadata to SmbIO.tap - it will reconstruct FileOperations fresh
        // This avoids closing over FileOperations which loses DatumFactory when serialized
        val tap = SmbIO.tap(dt.outputMetadata)(dt.valueCoder).apply(sc)
        val closedTap = com.spotify.scio.io.ClosedTap(tap)
        dt.setTap(closedTap)
      }
      // Call helper with existential type - the helper recaptures the type parameter
      populateTap(deferredTap)
    }

    SMBExecutionResult(
      pCollectionTuple = if (outputNodes.nonEmpty) Some(pCollectionTuple) else None,
      writeResults = writeResults
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
    fileOutputConsumersWithTags: Seq[
      (TupleTag[KV[BucketShardId, ResourceId]], SMBFileOutputConsumer[K1, K2, _])
    ],
    keyClassSecondary: Class[K2],
    metricsPrefix: String = "SMBCollection"
  )(implicit sc: ScioContext): org.apache.beam.sdk.values.PCollectionTuple = {
    val keying = if (keyClassSecondary == classOf[Void]) {
      SortedBucketSource.Keying.PRIMARY
    } else {
      SortedBucketSource.Keying.PRIMARY_AND_SECONDARY
    }
    val bucketedInputs = sources.map(_.toBucketedInput(keying))

    import org.apache.beam.sdk.extensions.smb.SMBCollectionHelper
    val sourceSpec = SMBCollectionHelper.createSourceSpec(bucketedInputs.asJava)

    // Use KV[K1, K2] for Beam compatibility
    val bucketSource = SMBCollectionHelper.createBucketSource[KV[K1, K2]](
      bucketedInputs.asJava,
      targetParallelism,
      1, // numShards
      0, // bucketOffset
      sourceSpec,
      -1 // keyCacheSize (use default)
    )

    val buckets =
      sc.pipeline.apply("SMB-Read-Buckets", org.apache.beam.sdk.io.Read.from(bucketSource))

    val someMetadata = bucketedInputs.head.getSourceMetadata.mapping.values.iterator.next.metadata

    // Use java.util.function.Function to avoid referencing package-private KeyFn type
    val keyFnAndComparator = if (keyClassSecondary == classOf[Void]) {
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
      if (keyClassSecondary == classOf[Void]) {
        // Primary only - beamKv is just K1, pair with Void
        (beamKv.asInstanceOf[K1], null.asInstanceOf[K2])
      } else {
        // Composite - beamKv is KV[K1, K2]
        val kv = beamKv.asInstanceOf[KV[K1, K2]]
        (kv.getKey, kv.getValue)
      }
    }

    // Extract serializable metadata from sources - do NOT serialize Beam objects!
    val sourceMetadata = sources.map(extractSourceMetadata)

    // Main tag for unused main output (Void output)
    val mainTag = new TupleTag[Void]("main-unused")

    val doFn = new SortedBucketMultiTransformDoFn[K1, K2](
      sourceMetadata, // Only serializable metadata, no Beam objects
      keying,
      keyFn,
      keyComparator,
      rootConsumer,
      sideInputs,
      metricsPrefix,
      fileOutputConsumersWithTags
    )

    // Upcast to TupleTag[_] to allow combining different tag types
    val allSideTags = outputNodes.map(_.tag.asInstanceOf[TupleTag[_]]) ++
      fileOutputConsumersWithTags.map(_._1.asInstanceOf[TupleTag[_]])
    val sideTagList =
      allSideTags.foldLeft(org.apache.beam.sdk.values.TupleTagList.empty) { (list, tag) =>
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
      val beamKeyCoder = if (keyClassSecondary == classOf[Void]) {
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

    result
  }

  /**
   * Finalization is skipped for SMBCollection because files are written directly to their final
   * location with metadata.json during consumer.finish().
   *
   * WriteResult is unused - taps are populated from OutputMetadata instead of Beam's file moving
   * infrastructure.
   *
   * @return
   *   null (safe because Scio never dereferences WriteResult for SMB taps)
   */
  def applyFinalization[K, K2, V](
    fileOutput: SMBSaveAsSortedBucketOutput[K, K2, V],
    bucketFiles: org.apache.beam.sdk.values.PCollection[KV[BucketShardId, ResourceId]]
  )(implicit sc: ScioContext): org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult = {
    val _ = (fileOutput, bucketFiles) // Suppress unused warnings
    null
  }
}
