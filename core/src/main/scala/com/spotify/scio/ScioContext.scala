package com.spotify.scio

import java.beans.Introspector
import java.io.File
import java.net.URI

import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.datastore.DatastoreV1.{Entity, Query}
import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder
import com.google.cloud.dataflow.sdk.io.{
  AvroIO => GAvroIO,
  BigQueryIO => GBigQueryIO,
  DatastoreIO => GDatastoreIO,
  PubsubIO => GPubsubIO,
  TextIO => GTextIO
}
import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled
import com.google.cloud.dataflow.sdk.options.{DataflowPipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
import com.google.cloud.dataflow.sdk.testing.TestPipeline
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn
import com.google.cloud.dataflow.sdk.transforms.{Create, PTransform}
import com.google.cloud.dataflow.sdk.values.{PBegin, PCollection, POutput, TimestampedValue}
import com.google.cloud.dataflow.sdk.{Pipeline, PipelineResult}
import com.spotify.scio.bigquery._
import com.spotify.scio.testing._
import com.spotify.scio.util.CallSites
import com.spotify.scio.values._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.Instant

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.reflect.ClassTag

/** Convenience object for creating [[ScioContext]] and [[Args]]. */
object ContextAndArgs {
  /** Create [[ScioContext]] and [[Args]] for command line arguments. */
  def apply(args: Array[String]): (ScioContext, Args) = {
    val context = ScioContext(args)
    (context, context.args)
  }
}

/** Companion object for [[ScioContext]]. */
object ScioContext {
  /**
   * Create a new [[ScioContext]] instance.
   *
   * @param args command line arguments including both Dataflow and job specific ones. Dataflow
   * specific ones will be parsed as
   * [[com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions DataflowPipelineOptions]] in
   * field `options`. Job specific ones will be parsed as [[Args]] in field `args`.
   */
  def apply(args: Array[String]): ScioContext = new ScioContext(args)
}

/**
 * Main entry point for Dataflow functionality. A ScioContext represents a Dataflow pipeline,
 * and can be used to create SCollections and distributed caches on that cluster.
 *
 * @groupname accumulator Accumulators
 * @groupname dist_cache Distributed Cache
 * @groupname in_memory In-memory Collections
 * @groupname input Input Sources
 * @groupname Ungrouped Other Members
 */
class ScioContext private (cmdlineArgs: Array[String]) {

  import Implicits._

  private var _options: DataflowPipelineOptions = null

  private var _result: PipelineResult = null

  private val (_args, _pipeline) = {
    val dfPatterns = classOf[DataflowPipelineOptions].getMethods.flatMap { m =>
      val n = m.getName
      if ((!n.startsWith("get") && !n.startsWith("is")) ||
        m.getParameterTypes.nonEmpty || m.getReturnType == classOf[Unit]) {
        None
      } else {
        Some(Introspector.decapitalize(n.substring(if (n.startsWith("is")) 2 else 3)))
      }
    }.map(s => s"--$s($$|=)".r)

    val (dfArgs, appArgs) = cmdlineArgs.partition(arg => dfPatterns.exists(_.findFirstIn(arg).isDefined))
    val _args = Args(appArgs)

    val _pipeline = if (_args.optional("testId").isDefined) {
      TestPipeline.create()
    } else {
      _options = PipelineOptionsFactory.fromArgs(dfArgs).as(classOf[DataflowPipelineOptions])
      options.setAppName(CallSites.getAppName)
      Pipeline.create(options)
    }
    _pipeline.getOptions.setStableUniqueNames(CheckEnabled.WARNING)
    _pipeline.getCoderRegistry.registerScalaCoders()

    (_args, _pipeline)
  }

  private lazy val bigQueryClient: BigQueryClient =
    BigQueryClient(this.options.getProject, this.options.getGcpCredential)

  /** Dataflow pipeline specific options, e.g. project, zone, runner, stagingLocation. */
  def options: DataflowPipelineOptions = _options

  /** Application specific arguments. */
  def args: Args = _args

  /** Dataflow pipeline. */
  def pipeline: Pipeline = _pipeline

  /** Close the context. */
  def close(): Unit = {
    _result = pipeline.run()
    complete()
  }

  /** Dataflow pipeline result. */
  def state: Option[State] = if (_result == null) None else Some(_result.getState)

  def isCompleted: Boolean = _result != null && _result.getState.isTerminal

  /** Wrap a [[com.google.cloud.dataflow.sdk.values.PCollection PCollection]]. */
  def wrap[T: ClassTag](p: PCollection[T]): SCollection[T] =
    new SCollectionImpl[T](p, this)

  // =======================================================================
  // Futures
  // =======================================================================

  private val _callbacks: ListBuffer[State => Any] = ListBuffer.empty

  private[scio] def onComplete[U](f: State => U): Unit = {
    _callbacks.append(f)
  }

  private def complete(): Unit = {
    if (pipeline.getRunner.isInstanceOf[DataflowPipelineRunner]) {
      import scala.concurrent.ExecutionContext.Implicits.global
      Future {
        while (!isCompleted) {
          Thread.sleep(1000)
        }
        _callbacks.foreach(_(_result.getState))
      }
    } else {
      _callbacks.foreach(_(_result.getState))
    }
  }

  // =======================================================================
  // Test wiring
  // =======================================================================

  private implicit def context: ScioContext = this

  private[scio] def isTest: Boolean = args.optional("testId").isDefined

  private[scio] def testIn: TestInput = TestDataManager.getInput(args("testId"))
  private[scio] def testOut: TestOutput = TestDataManager.getOutput(args("testId"))
  private[scio] def testDistCache: TestDistCache = TestDataManager.getDistCache(args("testId"))

  private def getTestInput[T: ClassTag](key: TestIO[T]): SCollection[T] =
    this.parallelize(testIn(key).asInstanceOf[Seq[T]])

  /* Read operations */

  private def applyInternal[Output <: POutput](root: PTransform[_ >: PBegin, Output]): Output =
    pipeline.apply(CallSites.getCurrent, root)

  /**
   * Get an SCollection of specific record type for an Avro file.
   * @group input
   */
  def avroFile[T: ClassTag](path: String): SCollection[T] =
    if (this.isTest) {
      this.getTestInput(AvroIO[T](path))
    } else {
      val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
      val o = this.applyInternal(GAvroIO.Read.from(path).withSchema(cls))
      wrap(o).setName(path)
    }

  /**
   * Get an SCollection of generic record type for an Avro file.
   * @group input
   */
  def avroFile(path: String, schema: Schema): SCollection[GenericRecord] =
    if (this.isTest) {
      this.getTestInput(AvroIO[GenericRecord](path))
    } else {
      wrap(this.applyInternal(GAvroIO.Read.from(path).withSchema(schema))).setName(path)
    }

  /**
   * Get an SCollection for a BigQuery SELECT query.
   * @group input
   */
  def bigQuerySelect(sqlQuery: String): SCollection[TableRow] =
    if (this.isTest) {
      this.getTestInput(BigQueryIO(sqlQuery))
    } else {
      val table = this.bigQueryClient.queryIntoTable(sqlQuery)
      bigQueryTable(table).setName(sqlQuery)
    }

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(table: TableReference): SCollection[TableRow] = {
    val tableSpec: String = GBigQueryIO.toTableSpec(table)
    if (this.isTest) {
      this.getTestInput(BigQueryIO(tableSpec))
    } else {
      wrap(this.applyInternal(GBigQueryIO.Read.from(table))).setName(tableSpec)
    }
  }

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(tableSpec: String): SCollection[TableRow] =
    this.bigQueryTable(GBigQueryIO.parseTableSpec(tableSpec))

  /**
   * Get an SCollection for a Datastore query.
   * @group input
   */
  def datastore(datasetId: String, query: Query): SCollection[Entity] =
    if (this.isTest) {
      this.getTestInput(DatastoreIO(datasetId, query))
    } else {
      wrap(this.applyInternal(GDatastoreIO.readFrom(datasetId, query)))
    }

  /**
   * Get an SCollection for a Pub/Sub subscription.
   * @group input
   */
  def pubsubSubscription(sub: String, idLabel: String = null, timestampLabel: String = null): SCollection[String] =
    if (this.isTest) {
      this.getTestInput(PubsubIO(sub))
    } else {
      var transform = GPubsubIO.Read.subscription(sub)
      if (idLabel != null) transform = transform.idLabel(idLabel)
      if (timestampLabel != null) transform = transform.timestampLabel(timestampLabel)
      wrap(this.applyInternal(transform)).setName(sub)
    }

  /**
   * Get an SCollection for a Pub/Sub topic.
   * @group input
   */
  def pubsubTopic(topic: String, idLabel: String = null, timestampLabel: String = null): SCollection[String] =
    if (this.isTest) {
      this.getTestInput(PubsubIO(topic))
    } else {
      var transform = GPubsubIO.Read.topic(topic)
      if (idLabel != null) transform = transform.idLabel(idLabel)
      if (timestampLabel != null) transform = transform.timestampLabel(timestampLabel)
      wrap(this.applyInternal(GPubsubIO.Read.topic(topic))).setName(topic)
    }

  /**
   * Get an SCollection of TableRow for a JSON file.
   * @group input
   */
  def tableRowJsonFile(path: String): SCollection[TableRow] =
    if (this.isTest) {
      this.getTestInput(TableRowJsonIO(path))
    } else {
      wrap(this.applyInternal(GTextIO.Read.from(path).withCoder(TableRowJsonCoder.of()))).setName(path)
    }

  /**
   * Get an SCollection for a text file.
   * @group input
   */
  def textFile[T](path: String): SCollection[String] =
    if (this.isTest) {
      this.getTestInput(TextIO(path))
    } else {
      wrap(this.applyInternal(GTextIO.Read.from(path))).setName(path)
    }

  // =======================================================================
  // Accumulators
  // =======================================================================

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the maximum value. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def maxAccumulator[U](n: String)(implicit at: AccumulatorType[U]): Accumulator[U] =
    new Accumulator[U] {
      override val name: String = n
      override val combineFn: CombineFn[U, _, U] = at.maxFn()
    }

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the minimum value. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def minAccumulator[U](n: String)(implicit at: AccumulatorType[U]): Accumulator[U] =
    new Accumulator[U] {
      override val name: String = n
      override val combineFn: CombineFn[U, _, U] = at.minFn()
    }

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the sum of values. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def sumAccumulator[U](n: String)(implicit at: AccumulatorType[U]): Accumulator[U] =
    new Accumulator[U] {
      override val name: String = n
      override val combineFn: CombineFn[U, _, U] = at.sumFn()
    }

  // =======================================================================
  // In-memory collections
  // =======================================================================

  /**
   * Distribute a local Scala Iterable to form an SCollection.
   * @group in_memory
   */
  def parallelize[T: ClassTag](elems: Iterable[T]): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    wrap(this.applyInternal(Create.of(elems.asJava).withCoder(coder))).setName(elems.toString())
  }

  /**
   * Distribute a local Scala Map to form an SCollection.
   * @group in_memory
   */
  def parallelize[K: ClassTag, V: ClassTag](elems: Map[K, V]): SCollection[(K, V)] = {
    val coder = pipeline.getCoderRegistry.getScalaKvCoder[K, V]
    wrap(this.applyInternal(Create.of(elems.asJava).withCoder(coder))).map(kv => (kv.getKey, kv.getValue))
      .setName(elems.toString())
  }

  /**
   * Distribute a local Scala Iterable with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[(T, Instant)]): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.map(t => TimestampedValue.of(t._1, t._2))
    wrap(this.applyInternal(Create.timestamped(v.asJava).withCoder(coder))).setName(elems.toString())
  }

  /**
   * Distribute a local Scala Iterable with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[T],
                                          timestamps: Iterable[Instant]): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.zip(timestamps).map(t => TimestampedValue.of(t._1, t._2))
    wrap(this.applyInternal(Create.timestamped(v.asJava).withCoder(coder))).setName(elems.toString())
  }

  // =======================================================================
  // Distributed cache
  // =======================================================================

  /**
   * Create a new [[com.spotify.scio.values.DistCache DistCache]] instance.
   * @param uri Google Cloud Storage URI of the file to be distributed to all workers
   * @param initFn function to initialized the distributed file
   *
   * {{{
   * // Prepare distributed cache as Map[Int, String]
   * val dc = sc.distCache("gs://dataflow-samples/samples/misc/months.txt") { f =>
   *   scala.io.Source.fromFile(f).getLines().map { s =>
   *     val t = s.split(" ")
   *     (t(0).toInt, t(1))
   *   }.toMap
   * }
   *
   * val p: SCollection[Int] = // ...
   * // Extract distributed cache inside a transform
   * p.map(x => dc().getOrElse(x, "unknown"))
   * }}}
   * @group dist_cache
   */
  def distCache[F](uri: String)(initFn: File => F): DistCache[F] =
    if (this.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uri)))
    } else {
      new DistCacheSingle(new URI(uri), initFn, options)
    }

  /**
   * Create a new [[com.spotify.scio.values.DistCache DistCache]] instance.
   * @param uris Google Cloud Storage URIs of the files to be distributed to all workers
   * @param initFn function to initialized the distributed files
   * @group dist_cache
   */
  def distCache[F](uris: Seq[String])(initFn: Seq[File] => F): DistCache[F] =
    if (this.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uris.mkString("\t"))))
    } else {
      new DistCacheMulti(uris.map(new URI(_)), initFn, options)
    }

}
