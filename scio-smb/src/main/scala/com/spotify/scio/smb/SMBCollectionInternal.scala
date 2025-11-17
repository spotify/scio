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
import com.spotify.scio.values.{SCollection, SideInput, SideInputContext}
import com.twitter.chill.ClosureCleaner
import org.apache.beam.sdk.extensions.smb.{BucketShardId, SortedBucketIO, TargetParallelism}
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver
import org.apache.beam.sdk.values.{KV, TupleTag}

import scala.collection.mutable

/** Push-based consumer for SMB outputs (file or PCollection). */
private[smb] trait SMBConsumer[K1, K2, -V] extends Serializable {
  def init(bucketId: Int, effectiveParallelism: Int): Unit
  def accept(key1: K1, key2: K2, value: V, receiver: MultiOutputReceiver): Unit
  def finish(): Unit

  // Default no-op; FanOutTransformingConsumer propagates to children
  def setSideInputContext(ctx: SideInputContext[_]): Unit = ()
}

/** Emits to PCollection via Beam's MultiOutputReceiver. */
private[smb] class SMBPCollectionConsumer[K1, K2, V](
  tag: TupleTag[_],
  isPrimaryOnly: Boolean
) extends SMBConsumer[K1, K2, V] {
  override def init(bucketId: Int, effectiveParallelism: Int): Unit = ()
  override def accept(key1: K1, key2: K2, value: V, receiver: MultiOutputReceiver): Unit = {
    if (isPrimaryOnly) {
      // Primary only - emit KV[K1, V]
      receiver.get(tag.asInstanceOf[TupleTag[KV[K1, V]]]).output(KV.of(key1, value))
    } else {
      // Composite - emit KV[KV[K1, K2], V]
      receiver
        .get(tag.asInstanceOf[TupleTag[KV[KV[K1, K2], V]]])
        .output(KV.of(KV.of(key1, key2), value))
    }
  }
  override def finish(): Unit = ()
}

/** Writes to temp SMB files (finalization DoFn moves them to final location). */
private[smb] class SMBFileOutputConsumer[K1, K2, V](
  outputMetadata: SMBCollectionImpl.OutputMetadata, // Store metadata, not FileOperations
  fileAssignment: org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment,
  sourceBucketMetadata: org.apache.beam.sdk.extensions.smb.BucketMetadata[
    _,
    _,
    _
  ], // For writing metadata.json
  metricsPrefix: String,
  outputIndex: Int // Index to identify this output in metrics
) extends SMBConsumer[K1, K2, V] {
  @transient private var writer: org.apache.beam.sdk.extensions.smb.FileOperations.Writer[V] = _
  @transient private var destination: ResourceId = _
  @transient private var bucketShardId: BucketShardId = _
  @transient private var bucketId: Int = _

  // Create metrics counter for this output
  private val recordsWritten: org.apache.beam.sdk.metrics.Counter =
    org.apache.beam.sdk.metrics.Metrics
      .counter(metricsPrefix, s"$metricsPrefix-RecordsWritten-Output-$outputIndex")

  override def init(bucketIdParam: Int, effectiveParallelism: Int): Unit = {
    import org.apache.beam.sdk.extensions.smb.SMBCollectionHelper
    bucketId = bucketIdParam
    bucketShardId = BucketShardId.of(bucketId, 0) // Shard 0 - no sharding within buckets
    destination =
      SMBCollectionHelper.forBucket(fileAssignment, bucketShardId, effectiveParallelism, 1)

    // Reconstruct FileOperations fresh from metadata to preserve DatumFactory
    val specificData = new org.apache.avro.specific.SpecificData()
    val recordClass = specificData
      .getClass(outputMetadata.schema)
      .asInstanceOf[Class[org.apache.avro.specific.SpecificRecord]]

    val fileOps = if (outputMetadata.isParquet) {
      // Reconstruct ParquetAvroFileOperations
      org.apache.beam.sdk.extensions.smb.ParquetAvroFileOperations
        .of(recordClass)
        .asInstanceOf[org.apache.beam.sdk.extensions.smb.FileOperations[V]]
    } else {
      // Reconstruct AvroFileOperations
      val datumFactory = new com.spotify.scio.avro.SpecificRecordDatumFactory(recordClass)
      org.apache.beam.sdk.extensions.smb.AvroFileOperations
        .of(datumFactory, outputMetadata.schema)
        .asInstanceOf[org.apache.beam.sdk.extensions.smb.FileOperations[V]]
    }

    writer = fileOps.createWriter(destination)
  }

  override def accept(key1: K1, key2: K2, value: V, receiver: MultiOutputReceiver): Unit = {
    writer.write(value)
    recordsWritten.inc()
  }

  override def finish(): Unit = {
    writer.close()

    // Write metadata.json for bucket 0 only (metadata is same for all buckets)
    if (bucketId == 0) {
      val metadataFile = fileAssignment.getDirectory.resolve(
        "metadata.json",
        org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE
      )

      // Write metadata using Beam's serialization
      val channel = org.apache.beam.sdk.io.FileSystems.create(metadataFile, "application/json")
      val outputStream = java.nio.channels.Channels.newOutputStream(channel)
      try {
        org.apache.beam.sdk.extensions.smb.BucketMetadata.to(sourceBucketMetadata, outputStream)
      } finally {
        outputStream.close()
      }
    }
  }
}

/** Applies transformation without fanout (single child or leaf node). */
private[smb] class TransformConsumer[K1, K2, In, Out](
  transform: (SideInputContext[_], K1, K2, In) => TraversableOnce[Out],
  child: SMBConsumer[K1, K2, Out]
) extends SMBConsumer[K1, K2, In] {
  @transient private var sideInputCtx: SideInputContext[_] = _

  override def init(bucketId: Int, effectiveParallelism: Int): Unit =
    child.init(bucketId, effectiveParallelism)

  override def accept(key1: K1, key2: K2, value: In, receiver: MultiOutputReceiver): Unit = {
    val transformed = transform(sideInputCtx, key1, key2, value)
    transformed.foreach { out =>
      child.accept(key1, key2, out, receiver)
    }
  }

  override def finish(): Unit =
    child.finish()

  override def setSideInputContext(ctx: SideInputContext[_]): Unit = {
    sideInputCtx = ctx
    child.setSideInputContext(ctx)
  }
}

/** No-op consumer for side-effect-only transforms (e.g., .tap() with no outputs). */
private[smb] object NoOpConsumer {
  def apply[K1, K2, V]: SMBConsumer[K1, K2, V] = new SMBConsumer[K1, K2, V] {
    override def init(bucketId: Int, effectiveParallelism: Int): Unit = ()
    override def accept(key1: K1, key2: K2, value: V, receiver: MultiOutputReceiver): Unit = ()
    override def finish(): Unit = ()
    override def setSideInputContext(ctx: SideInputContext[_]): Unit = ()
  }
}

/** Passes values through to multiple children without transformation (fanout only). */
private[smb] class PassThroughConsumer[K1, K2, V](
  children: Seq[SMBConsumer[K1, K2, V]]
) extends SMBConsumer[K1, K2, V] {

  override def init(bucketId: Int, effectiveParallelism: Int): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).init(bucketId, effectiveParallelism)
      i += 1
    }
  }

  override def accept(key1: K1, key2: K2, value: V, receiver: MultiOutputReceiver): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).accept(key1, key2, value, receiver)
      i += 1
    }
  }

  override def finish(): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).finish()
      i += 1
    }
  }

  override def setSideInputContext(ctx: SideInputContext[_]): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).setSideInputContext(ctx)
      i += 1
    }
  }
}

/** Result of executing an SMB graph (PCollection outputs + file write results). */
private[smb] case class SMBExecutionResult(
  pCollectionTuple: Option[org.apache.beam.sdk.values.PCollectionTuple],
  writeResults: Map[
    SMBSaveAsSortedBucketOutput[_, _, _],
    org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult
  ]
)

/**
 * Deferred tap that gets populated during graph construction.
 *
 * Execution flow:
 * {{{
 *   val smbCollection = SMBCollection.read(...)
 *   val deferredTap = smbCollection.saveAsSortedBucket(...)
 *
 *   // More operations on the smbCollection
 *
 *   // 1. GRAPH CONSTRUCTION: Seal the collection and build the pipeline graph.
 *   //    This creates the DoFn and all taps (but doesn't execute the pipeline).
 *   //    Can be triggered by deferredTap.get OR by sc.onClose callback.
 *   val closedTap = deferredTap.get
 *
 *   // 2. PIPELINE EXECUTION: Run the pipeline (executes the DoFn, writes files).
 *   val result = sc.run()
 *
 *   // 3. TAP MATERIALIZATION: Access the written data via the tap.
 *   val data = result.tap(closedTap)
 * }}}
 *
 * The tap is created during graph construction (step 1) with knowledge of where files will be
 * written (via WriteResult metadata). The pipeline then executes (step 2), and finally the tap
 * reads the written files (step 3).
 */
private[smb] class DeferredTap[V](
  rootContext: SMBRootContext[_, _],
  val outputNode: SMBSaveAsSortedBucketOutput[_, _, V],
  val valueCoder: Coder[V], // Explicitly store coder to preserve type information
  val outputMetadata: SMBCollectionImpl.OutputMetadata // Metadata for reconstructing FileOperations
) extends Deferred[com.spotify.scio.io.ClosedTap[V]] {
  @volatile private var tapOption: Option[com.spotify.scio.io.ClosedTap[V]] = None

  private[smb] def setTap(tap: com.spotify.scio.io.ClosedTap[V]): Unit =
    tapOption = Some(tap)

  override def get: com.spotify.scio.io.ClosedTap[V] = {
    // Seal and execute if not already done (GRAPH CONSTRUCTION phase)
    rootContext.sealAndExecute()

    // Tap should have been populated during graph construction
    tapOption.getOrElse(
      throw new IllegalStateException(s"Tap not populated for output: ${outputNode.output}")
    )
  }
}

/** Root execution context - manages graph execution and caches results for deferred SCollections. */
private[smb] class SMBRootContext[K1, K2](
  val sources: List[SortedBucketIO.Read[_]],
  val targetParallelism: TargetParallelism,
  val keyClass: Class[K1],
  val keyClassSecondary: Class[K2]
)(implicit val sc: ScioContext) {
  // Core is set after construction to break circular dependency
  // Root core has In=List[Iterable[Any]] (from SimpleKeyGroupReader), Out=List[Iterable[Any]] (identity)
  private[smb] var core: SMBCollectionCore[K1, K2, List[Iterable[Any]], List[Iterable[Any]]] = _

  @volatile private[smb] var isSealed: Boolean = false
  @volatile private[smb] var executionResult: Option[SMBExecutionResult] = None

  // Track deferred taps that need to be populated during execution
  private[smb] val deferredTaps = mutable.Buffer.empty[DeferredTap[_]]

  /** Set the root core (called once during construction). */
  private[smb] def setCore(
    c: SMBCollectionCore[K1, K2, List[Iterable[Any]], List[Iterable[Any]]]
  ): Unit = {
    require(core == null, "Core already set - this is a bug")
    core = c
  }

  /**
   * Seal the graph and execute it, returning the SMBExecutionResult with all outputs. If already
   * executed, returns the cached result.
   */
  def sealAndExecute(): SMBExecutionResult = {
    if (!isSealed) {
      synchronized {
        if (!isSealed) {
          isSealed = true
          val allSideInputs = core.collectAllSideInputs() // Collect from entire tree, not just root
          val result = SMBCollectionImpl.executeGraph(
            sources,
            targetParallelism,
            core,
            allSideInputs,
            keyClassSecondary
          )(sc)
          executionResult = Some(result)
          result
        } else {
          executionResult.get
        }
      }
    } else {
      executionResult.getOrElse(
        throw new IllegalStateException("Graph sealed but not executed - this is a bug")
      )
    }
  }
}

/** Deferred SCollection - graph executes on first `get`, subsequent calls reuse cached result. */
private[smb] class SMBDeferredSCollection[K1: Coder, K2: Coder, V: Coder](
  rootContext: SMBRootContext[K1, K2],
  outputNode: SMBToSCollectionOutput[K1, K2, V],
  key1Coder: Coder[K1],
  key2Coder: Coder[K2],
  valueCoder: Coder[V]
) extends Deferred[SCollection[((K1, K2), V)]] {

  def get: SCollection[((K1, K2), V)] = {
    // Seal and execute if not already done
    val result = rootContext.sealAndExecute()

    // Extract PCollectionTuple (must exist if we have PCollection outputs)
    val tuple = result.pCollectionTuple.getOrElse(
      throw new IllegalStateException("No PCollection outputs in execution result")
    )

    // Convert to SCollection - Beam uses KV[K1, V] or KV[KV[K1, K2], V]
    if (rootContext.keyClassSecondary == classOf[Void]) {
      // Primary key only - beamPc is PCollection[KV[K1, V]]
      val typedPc =
        tuple.get(outputNode.tag).asInstanceOf[org.apache.beam.sdk.values.PCollection[KV[K1, V]]]

      // Create custom coder that encodes only K1 (not Void) in the key tuple
      // This avoids VoidCoder.decode being called
      implicit val keyCoder: Coder[(K1, K2)] = Coder.xmap(key1Coder)(
        k1 => (k1, null.asInstanceOf[K2]), // encode: K1 => (K1, Void)
        keyTuple => keyTuple._1 // decode: (K1, Void) => K1
      )
      implicit val tupleCoder: Coder[((K1, K2), V)] = Coder.tuple2Coder(keyCoder, valueCoder)

      rootContext.sc.wrap(typedPc).map { kv =>
        val k1 = kv.getKey
        val v = kv.getValue
        ((k1, null.asInstanceOf[K2]), v)
      }
    } else {
      // Composite key - beamPc is PCollection[KV[KV[K1, K2], V]]
      val typedPc = tuple
        .get(outputNode.tag)
        .asInstanceOf[org.apache.beam.sdk.values.PCollection[KV[KV[K1, K2], V]]]
      implicit val tupleCoder: Coder[((K1, K2), V)] = Coder.tuple2Coder(
        Coder.tuple2Coder(key1Coder, key2Coder),
        valueCoder
      )
      rootContext.sc.wrap(typedPc).map { kv =>
        val beamKey = kv.getKey
        val v = kv.getValue
        ((beamKey.getKey, beamKey.getValue), v)
      }
    }
  }
}

/**
 * Core tree node - stores transformation, children, and shared root context. Thin wrappers delegate
 * to this.
 */
sealed private[smb] class SMBCollectionCore[K1: Coder, K2: Coder, In, Out: Coder](
  private[smb] val transformFn: Option[(SideInputContext[_], K1, K2, In) => TraversableOnce[Out]],
  private[smb] val rootContext: SMBRootContext[K1, K2], // Shared by entire graph
  private[smb] val sideInputs: Seq[SideInput[_]] = Seq.empty // Immutable per node
) extends Serializable {

  private[smb] val children = mutable.Buffer.empty[SMBCollectionCore[K1, K2, Out, _]]
  private[smb] val key1Coder: Coder[K1] = implicitly[Coder[K1]]
  private[smb] val key2Coder: Coder[K2] = implicitly[Coder[K2]]
  val valueCoder: Coder[Out] = implicitly[Coder[Out]]

  lazy val keyed: SMBCollection[K1, K2, Out] = new SMBCollectionKeyed[K1, K2, Out](this)
  lazy val values: SMBCollectionValues[K1, K2, Out] =
    new SMBCollectionValuesWrapper[K1, K2, Out](this)

  /** Create transformation node. */
  def transform[W: Coder](
    f: (SideInputContext[_], K1, K2, Out) => TraversableOnce[W]
  ): SMBCollectionCore[K1, K2, Out, W] = {
    val child = new SMBCollectionCore[K1, K2, Out, W](
      transformFn = Some(ClosureCleaner.clean(f)),
      rootContext = rootContext
    )(key1Coder, key2Coder, implicitly[Coder[W]])
    children += child
    child
  }

  /**
   * Create a deferred SCollection that will be materialized on first access. Multiple deferred
   * SCollections can share the same execution.
   */
  def toDeferredSCollection(): Deferred[SCollection[((K1, K2), Out)]] = {
    // Create output node as a child
    val outputNode = new SMBToSCollectionOutput[K1, K2, Out](rootContext)
    children += outputNode

    // Return deferred wrapper
    new SMBDeferredSCollection[K1, K2, Out](
      rootContext,
      outputNode,
      key1Coder,
      key2Coder,
      valueCoder
    )
  }

  /**
   * Attach a saveAsSortedBucket output to this node. Returns a Deferred[ClosedTap] that will
   * provide access to the written data.
   *
   * Usage pattern:
   * {{{
   *   val deferredTap = smbCollection.saveAsSortedBucket(output)
   *   val closedTap = deferredTap.get  // Seals graph, creates taps (before sc.run())
   *   val result = sc.run()             // Executes pipeline, writes files
   *   val data = result.tap(closedTap)  // Reads written files
   * }}}
   *
   * The tap is created during graph construction (.get call) with metadata about where files will
   * be written. The pipeline then executes (sc.run()), and the tap can be materialized to read the
   * written files (result.tap()).
   */
  def attachSaveAsSortedBucket(
    output: SortedBucketIO.TransformOutput[K1, K2, Out]
  ): Deferred[com.spotify.scio.io.ClosedTap[Out]] = {
    // Create output node as a child
    val outputNode = new SMBSaveAsSortedBucketOutput[K1, K2, Out](output, rootContext)
    children += outputNode

    // Get FileOperations to create coder with DatumFactory
    import org.apache.beam.sdk.extensions.smb.SMBCollectionHelper
    val fileOps = SMBCollectionHelper.getFileOperations(output)
    val beamCoder = fileOps.getCoder()
    val coderWithDatumFactory = Coder.beam[Out](beamCoder)

    // Extract output metadata for reconstructing FileOperations in tap
    val outputMetadata = SMBCollectionImpl.extractOutputMetadata(output)

    // Create deferred tap and register it with root context
    val deferredTap =
      new DeferredTap[Out](rootContext, outputNode, coderWithDatumFactory, outputMetadata)
    rootContext.deferredTaps += deferredTap

    deferredTap
  }

  /**
   * Collect all side inputs from this node and all descendants. Returns a deduplicated sequence of
   * all side inputs in the tree.
   */
  def collectAllSideInputs(): Seq[SideInput[_]] = {
    val set = mutable.Set.empty[SideInput[_]]
    collectAllSideInputsRecursive(set)
    set.toSeq
  }

  private def collectAllSideInputsRecursive(acc: mutable.Set[SideInput[_]]): Unit = {
    acc ++= sideInputs
    children.foreach(_.collectAllSideInputsRecursive(acc))
  }

  /** Build consumer for this node. Output nodes override to create specialized consumers. */
  def buildConsumer(
    outputNodes: mutable.Buffer[SMBToSCollectionOutput[K1, K2, _]],
    fileOutputConsumers: mutable.Buffer[
      (SMBSaveAsSortedBucketOutput[K1, K2, _], SMBFileOutputConsumer[K1, K2, _])
    ],
    metricsPrefix: String = "SMBCollection" // Default prefix for metrics
  ): SMBConsumer[K1, K2, In] = {
    // Recursively build child consumers
    val childConsumers: Seq[SMBConsumer[K1, K2, Out]] = children.map { child =>
      child.buildConsumer(outputNodes, fileOutputConsumers, metricsPrefix)
    }.toSeq

    // Phase 1: Build fanout consumer based on number of children
    val fanoutConsumer: SMBConsumer[K1, K2, Out] = childConsumers.size match {
      case 0 => NoOpConsumer[K1, K2, Out]
      case 1 => childConsumers.head
      case _ => new PassThroughConsumer[K1, K2, Out](childConsumers)
    }

    // Phase 2: Optionally wrap with transform consumer
    transformFn match {
      case None =>
        // No transformation - return fanout consumer directly (with type cast)
        fanoutConsumer.asInstanceOf[SMBConsumer[K1, K2, In]]

      case Some(transform) =>
        // Has transformation - wrap fanout consumer
        // transformFn signature is (SideInputContext, K1, K2, In) => TraversableOnce[Out]
        new TransformConsumer[K1, K2, In, Out](
          transform,
          fanoutConsumer
        )
    }
  }
}

/**
 * Output node for toSCollectionAndSeal. Extends SMBCollectionCore so it can be a child in the tree.
 * Creates its own TupleTag for identifying this output in Beam's MultiOutputReceiver.
 */
private[smb] class SMBToSCollectionOutput[K1: Coder, K2: Coder, V: Coder](
  rootContext: SMBRootContext[K1, K2]
) extends SMBCollectionCore[K1, K2, V, V](
      transformFn = None, // No transformation - pure output node
      rootContext = rootContext
    ) {
  // Beam tag uses KV[K1, K2] or K1 depending on whether secondary key is used
  val tag: TupleTag[_] = if (rootContext.keyClassSecondary == classOf[Void]) {
    new TupleTag[KV[K1, V]]()
  } else {
    new TupleTag[KV[KV[K1, K2], V]]()
  }

  /** Build PCollection consumer. */
  override def buildConsumer(
    outputNodes: mutable.Buffer[SMBToSCollectionOutput[K1, K2, _]],
    fileOutputConsumers: mutable.Buffer[
      (SMBSaveAsSortedBucketOutput[K1, K2, _], SMBFileOutputConsumer[K1, K2, _])
    ],
    metricsPrefix: String = "SMBCollection"
  ): SMBConsumer[K1, K2, V] = {
    outputNodes += this
    val isPrimaryOnly = rootContext.keyClassSecondary == classOf[Void]
    new SMBPCollectionConsumer[K1, K2, V](tag, isPrimaryOnly)
  }
}

/** Output node for saveAsSortedBucket. */
private[smb] class SMBSaveAsSortedBucketOutput[K1: Coder, K2: Coder, V: Coder](
  val output: SortedBucketIO.TransformOutput[K1, K2, V],
  rootContext: SMBRootContext[K1, K2]
) extends SMBCollectionCore[K1, K2, V, V](
      transformFn = None, // No transformation - pure output node
      rootContext = rootContext
    ) {

  /** Build file output consumer. */
  override def buildConsumer(
    outputNodes: mutable.Buffer[SMBToSCollectionOutput[K1, K2, _]],
    fileOutputConsumers: mutable.Buffer[
      (SMBSaveAsSortedBucketOutput[K1, K2, _], SMBFileOutputConsumer[K1, K2, _])
    ],
    metricsPrefix: String = "SMBCollection"
  ): SMBConsumer[K1, K2, V] = {
    // Use helper to access package-private methods
    import org.apache.beam.sdk.extensions.smb.SMBCollectionHelper
    val outputDir = SMBCollectionHelper.getOutputDirectory(output)

    // Extract metadata for reconstructing FileOperations later
    val outputMetadata = SMBCollectionImpl.extractOutputMetadata(output)

    // Create filename policy for this output
    val filenamePolicy = new org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy(
      outputDir,
      SMBCollectionHelper.getFilenamePrefix(output),
      SMBCollectionHelper.getFilenameSuffix(output)
    )

    // Write directly to final location (skip temp/finalization to avoid RenameBuckets reading files)
    // This avoids the DatumFactory serialization issue in RenameBuckets
    val fileAssignment = filenamePolicy.forDestination()

    // Get source bucket metadata for writing metadata.json
    val sourceMetadata = SMBCollectionImpl.extractSourceMetadata(rootContext.sources.head)
    val sourceBucketMetadata = {
      val freshSource = SMBCollectionImpl.reconstructRead(sourceMetadata)
      val bucketedInput = freshSource.toBucketedInput(
        org.apache.beam.sdk.extensions.smb.SortedBucketSource.Keying.PRIMARY
      )
      bucketedInput.getSourceMetadata.mapping.values.iterator.next.metadata
    }

    // outputIndex is the current number of file outputs (used for unique metric names)
    val outputIndex = fileOutputConsumers.size

    // Create the file output consumer with metadata instead of FileOperations
    val consumer =
      new SMBFileOutputConsumer[K1, K2, V](
        outputMetadata,
        fileAssignment,
        sourceBucketMetadata,
        metricsPrefix,
        outputIndex
      )

    // Track this consumer for finalization
    fileOutputConsumers += ((this, consumer))

    consumer
  }
}

// ============================================
// Thin wrapper implementations
// ============================================

/** Keyed view - implements SMBCollection[K, Void, V] trait. All methods delegate to the core. */
private[smb] class SMBCollectionKeyed[K1: Coder, K2: Coder, V: Coder](
  private val core: SMBCollectionCore[K1, K2, _, V]
) extends SMBCollection[K1, K2, V] {

  override def flatMap[W: Coder](f: (K1, K2, V) => TraversableOnce[W]): SMBCollection[K1, K2, W] =
    core
      .transform[W] { (_: SideInputContext[_], k1: K1, k2: K2, v: V) =>
        f(k1, k2, v)
      }
      .keyed

  override def values: SMBCollectionValues[K1, K2, V] = core.values

  override def saveAsSortedBucket(
    output: SortedBucketIO.TransformOutput[K1, K2, V]
  ): Deferred[com.spotify.scio.io.ClosedTap[V]] =
    core.attachSaveAsSortedBucket(output)

  override def toDeferredSCollection()(implicit
    sc: ScioContext,
    coder: Coder[V]
  ): Deferred[SCollection[((K1, K2), V)]] =
    core.toDeferredSCollection()

  override def withSideInputs(sides: SideInput[_]*): SMBCollectionWithSideInputs[K1, K2, V] = {
    // Passthrough node isolates side inputs (prevents conflicts from multiple withSideInputs calls)
    val passthroughCore = new SMBCollectionCore[K1, K2, V, V](
      transformFn = None, // No transformation - just passes values through with side input context
      rootContext = core.rootContext,
      sideInputs = sides.toSeq
    )(core.key1Coder, core.key2Coder, core.valueCoder)
    core.children += passthroughCore
    new SMBCollectionWithSideInputsWrapper[K1, K2, V](passthroughCore, sides.toSeq)
  }
}

/** Values view wrapper. */
private[smb] class SMBCollectionValuesWrapper[K1: Coder, K2: Coder, V: Coder](
  private val core: SMBCollectionCore[K1, K2, _, V]
) extends SMBCollectionValues[K1, K2, V] {

  override def flatMap[W: Coder](f: V => TraversableOnce[W]): SMBCollectionValues[K1, K2, W] =
    core
      .transform[W] { (_: SideInputContext[_], _: K1, _: K2, v: V) =>
        f(v)
      }
      .values

  override def saveAsSortedBucket(
    output: SortedBucketIO.TransformOutput[K1, K2, V]
  ): Deferred[com.spotify.scio.io.ClosedTap[V]] =
    core.attachSaveAsSortedBucket(output)

  override def keyed: SMBCollection[K1, K2, V] = core.keyed

  override def toDeferredSCollection()(implicit
    sc: ScioContext,
    coder: Coder[V]
  ): Deferred[SCollection[V]] =
    core.toDeferredSCollection().map(_.map(_._2)(coder))
}

/** WithSideInputs view wrapper. */
private[smb] class SMBCollectionWithSideInputsWrapper[K1: Coder, K2: Coder, V: Coder](
  private val core: SMBCollectionCore[K1, K2, _, V],
  sides: Seq[SideInput[_]]
) extends SMBCollectionWithSideInputs[K1, K2, V] {

  override def flatMap[W: Coder](
    f: (SideInputContext[_], K1, K2, V) => TraversableOnce[W]
  ): SMBCollectionWithSideInputs[K1, K2, W] = {
    val newCore = new SMBCollectionCore[K1, K2, V, W](
      Some(ClosureCleaner.clean(f)),
      core.rootContext,
      sides
    )(core.key1Coder, core.key2Coder, implicitly[Coder[W]])
    core.children += newCore
    new SMBCollectionWithSideInputsWrapper[K1, K2, W](newCore, sides)
  }

  override def values: SMBCollectionWithSideInputsValues[K1, K2, V] =
    new SMBCollectionWithSideInputsValuesWrapper[K1, K2, V](core, sides)

  override def saveAsSortedBucket(
    output: SortedBucketIO.TransformOutput[K1, K2, V]
  ): Deferred[com.spotify.scio.io.ClosedTap[V]] =
    core.attachSaveAsSortedBucket(output)

  override def toDeferredSCollection()(implicit
    sc: ScioContext,
    coder: Coder[V]
  ): Deferred[SCollection[((K1, K2), V)]] =
    core.toDeferredSCollection()
}

/** WithSideInputsValues view wrapper. */
private[smb] class SMBCollectionWithSideInputsValuesWrapper[K1: Coder, K2: Coder, V: Coder](
  private val core: SMBCollectionCore[K1, K2, _, V],
  sides: Seq[SideInput[_]]
) extends SMBCollectionWithSideInputsValues[K1, K2, V] {

  override def flatMap[W: Coder](
    f: (SideInputContext[_], V) => TraversableOnce[W]
  ): SMBCollectionWithSideInputsValues[K1, K2, W] = {
    val newCore = new SMBCollectionCore[K1, K2, V, W](
      Some(ClosureCleaner.clean((ctx: SideInputContext[_], _: K1, _: K2, v: V) => f(ctx, v))),
      core.rootContext,
      sides
    )(core.key1Coder, core.key2Coder, implicitly[Coder[W]])
    core.children += newCore
    new SMBCollectionWithSideInputsValuesWrapper[K1, K2, W](newCore, sides)
  }

  override def keyed: SMBCollectionWithSideInputs[K1, K2, V] =
    new SMBCollectionWithSideInputsWrapper[K1, K2, V](core, sides)

  override def saveAsSortedBucket(
    output: SortedBucketIO.TransformOutput[K1, K2, V]
  ): Deferred[com.spotify.scio.io.ClosedTap[V]] =
    core.attachSaveAsSortedBucket(output)

  override def toDeferredSCollection()(implicit
    sc: ScioContext,
    coder: Coder[V]
  ): Deferred[SCollection[V]] =
    core.toDeferredSCollection().map(_.map(_._2)(coder))
}
