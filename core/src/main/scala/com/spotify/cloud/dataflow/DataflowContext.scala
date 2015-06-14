package com.spotify.cloud.dataflow

import java.beans.Introspector
import java.io.File
import java.net.URI

import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.datastore.DatastoreV1.{Query, Entity}
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder
import com.google.cloud.dataflow.sdk.io.{
  AvroIO => GAvroIO,
  BigQueryIO => GBigQueryIO,
  DatastoreIO => GDatastoreIO,
  PubsubIO => GPubsubIO,
  TextIO => GTextIO
}
import com.google.cloud.dataflow.sdk.options.{PipelineOptionsFactory, DataflowPipelineOptions}
import com.google.cloud.dataflow.sdk.testing.TestPipeline
import com.google.cloud.dataflow.sdk.transforms.{PTransform, Create}
import com.google.cloud.dataflow.sdk.values.{TimestampedValue, PBegin, POutput}
import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow.testing._
import com.spotify.cloud.dataflow.util.CallSites
import com.spotify.cloud.dataflow.values._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.joda.time.Instant

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/** Convenience object for creating [[DataflowContext]] and [[Args]]. */
object ContextAndArgs {
  /** Create [[DataflowContext]] and [[Args]] for command line arguments. */
  def apply(args: Array[String]): (DataflowContext, Args) = {
    val context = DataflowContext(args)
    (context, context.args)
  }
}

/** Companion object for [[DataflowContext]]. */
object DataflowContext {
  /**
   * Create a new [[DataflowContext]] instance.
   *
   * @param args command line arguments including both Dataflow and job specific ones. Dataflow
   * specific ones will be parsed as
   * [[com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions DataflowPipelineOptions]] in
   * field `options`. Job specific ones will be parsed as [[Args]] in field `args`.
   */
  def apply(args: Array[String]): DataflowContext = new DataflowContext(args)
}

/**
 * Main entry point for Dataflow functionality. A DataflowContext represents a Dataflow pipeline,
 * and can be used to create SCollections and distributed caches on that cluster.
 */
class DataflowContext private (cmdlineArgs: Array[String]) {

  import PrivateImplicits._

  private var _options: DataflowPipelineOptions = null

  private var _sqlId: Int = 0

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
      _options = PipelineOptionsFactory.fromArgs(dfArgs).withValidation().as(classOf[DataflowPipelineOptions])
      options.setAppName(CallSites.getAppName)
      Pipeline.create(options)
    }
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
  def close(): Unit = pipeline.run()

  // =======================================================================
  // Test wiring
  // =======================================================================

  private implicit def context: DataflowContext = this

  private[dataflow] def isTest: Boolean = args.optional("testId").isDefined

  private[dataflow] def testIn: TestInput = TestDataManager.getInput(args("testId"))
  private[dataflow] def testOut: TestOutput = TestDataManager.getOutput(args("testId"))
  private[dataflow] def testDistCache: TestDistCache = TestDataManager.getDistCache(args("testId"))

  private def getTestInput[T: ClassTag](key: TestIO[T]): SCollection[T] =
    this.parallelize(testIn(key).asInstanceOf[Seq[T]])

  /* Read operations */

  private def applyInternal[Output <: POutput](root: PTransform[_ >: PBegin, Output]): Output =
    pipeline.apply(root.setName(CallSites.getCurrent))

  /** Get an SCollection of specific record type for an Avro file. */
  def avroFile[T <: IndexedRecord: ClassTag](path: String): SCollection[T] =
    if (this.isTest) {
      this.getTestInput(AvroIO[T](path))
    } else {
      val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
      val o = this.applyInternal(GAvroIO.Read.from(path).withSchema(cls))
      SCollection(o).setName(path)
    }

  /** Get an SCollection of generic record type for an Avro file. */
  def avroFile(path: String, schema: Schema): SCollection[GenericRecord] =
    if (this.isTest) {
      this.getTestInput(AvroIO[GenericRecord](path))
    } else {
      SCollection(this.applyInternal(GAvroIO.Read.from(path).withSchema(schema))).setName(path)
    }

  /** Get an SCollection of generic record type for an Avro file. */
  def avroFile(path: String, schemaString: String): SCollection[GenericRecord] =
    if (this.isTest) {
      this.getTestInput(AvroIO[GenericRecord](path))
    } else {
      SCollection(this.applyInternal(GAvroIO.Read.from(path).withSchema(schemaString))).setName(path)
    }

  /** Get an SCollection for a BigQuery SELECT query. */
  def bigQuerySelect(sqlQuery: String): SCollection[TableRow] =
    if (this.isTest) {
      this.getTestInput(BigQueryIO(sqlQuery))
    } else {
      val table = this.bigQueryClient.queryIntoTable(sqlQuery)
      bigQueryTable(table).setName(sqlQuery)
    }

  /** Get an SCollection for a BigQuery table. */
  def bigQueryTable(table: TableReference): SCollection[TableRow] = {
    val tableSpec: String = GBigQueryIO.toTableSpec(table)
    if (this.isTest) {
      this.getTestInput(BigQueryIO(tableSpec))
    } else {
      SCollection(this.applyInternal(GBigQueryIO.Read.from(table))).setName(tableSpec)
    }
  }

  /** Get an SCollection for a BigQuery table. */
  def bigQueryTable(tableSpec: String): SCollection[TableRow] =
    this.bigQueryTable(GBigQueryIO.parseTableSpec(tableSpec))

  /** Get an SCollection for a Datastore query. */
  def datastore(datasetId: String, query: Query): SCollection[Entity] =
    if (this.isTest) {
      this.getTestInput(DatastoreIO(datasetId, query))
    } else {
      SCollection(this.applyInternal(GDatastoreIO.readFrom(datasetId, query)))
    }

  /** Get an SCollection for a Pub/Sub subscription. */
  def pubsubSubscription(subscription: String): SCollection[String] =
    if (this.isTest) {
      this.getTestInput(PubsubIO(subscription))
    } else {
      SCollection(this.applyInternal(GPubsubIO.Read.subscription(subscription)))
        .setName(subscription)
    }

  /** Get an SCollection for a Pub/Sub topic. */
  def pubsubTopic(topic: String): SCollection[String] =
    if (this.isTest) {
      this.getTestInput(PubsubIO(topic))
    } else {
      SCollection(this.applyInternal(GPubsubIO.Read.topic(topic))).setName(topic)
    }

  /** Get an SCollection of TableRow for a JSON file. */
  def tableRowJsonFile(path: String): SCollection[TableRow] =
    if (this.isTest) {
      this.getTestInput(TableRowJsonIO(path))
    } else {
      SCollection(this.applyInternal(GTextIO.Read.from(path).withCoder(TableRowJsonCoder.of()))).setName(path)
    }

  /** Get an SCollection for a text file. */
  def textFile[T](path: String): SCollection[String] =
    if (this.isTest) {
      this.getTestInput(TextIO(path))
    } else {
      SCollection(this.applyInternal(GTextIO.Read.from(path))).setName(path)
    }

  // =======================================================================
  // In-memory collections
  // =======================================================================

  /** Distribute local values to form an SCollection. */
  def parallelize[T: ClassTag](elems: T*): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    SCollection(this.applyInternal(Create.of(elems: _*)).setCoder(coder)).setName(elems.toString())
  }

  /** Distribute a local Scala Iterable to form an SCollection. */
  def parallelize[T: ClassTag](elems: Iterable[T]): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    SCollection(this.applyInternal(Create.of(elems.asJava)).setCoder(coder)).setName(elems.toString())
  }

  /** Distribute a local Scala Map to form an SCollection. */
  def parallelize[K: ClassTag, V: ClassTag](elems: Map[K, V]): SCollection[(K, V)] = {
    val coder = pipeline.getCoderRegistry.getScalaKvCoder[K, V]
    SCollection(this.applyInternal(Create.of(elems.asJava)).setCoder(coder)).map(kv => (kv.getKey, kv.getValue))
      .setName(elems.toString())
  }

  /** Distribute local values with timestamps to form an SCollection. */
  def parallelizeTimestamped[T: ClassTag](elems: (T, Instant)*): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.map(t => TimestampedValue.of(t._1, t._2))
    SCollection(this.applyInternal(Create.timestamped(v: _*)).setCoder(coder)).setName(elems.toString())
  }

  /** Distribute a local Scala Iterable with timestamps to form an SCollection. */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[(T, Instant)]): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.map(t => TimestampedValue.of(t._1, t._2))
    SCollection(this.applyInternal(Create.timestamped(v.asJava)).setCoder(coder)).setName(elems.toString())
  }

  /** Distribute a local Scala Iterable with timestamps to form an SCollection. */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[T],
                                          timestamps: Iterable[Instant]): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.zip(timestamps).map(t => TimestampedValue.of(t._1, t._2))
    SCollection(this.applyInternal(Create.timestamped(v.asJava)).setCoder(coder)).setName(elems.toString())
  }

  // =======================================================================
  // Distributed cache
  // =======================================================================

  /**
   * Create a new [[com.spotify.cloud.dataflow.values.DistCache DistCache]] instance.
   * @param uri Google Cloud Storage URI of the file to be distributed to all workers
   * @param initFn function to initialized the distributed file
   *
   * {{{
   * // Prepare distributed cache as Map[Int, String]
   * val dc = context.distCache("gs://dataflow-samples/samples/misc/months.txt") { f =>
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
   */
  def distCache[F](uri: String)(initFn: File => F): DistCache[F] =
    if (this.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uri)))
    } else {
      new DistCacheSingle(new URI(uri), initFn, options)
    }

  /**
   * Create a new [[com.spotify.cloud.dataflow.values.DistCache DistCache]] instance.
   * @param uris Google Cloud Storage URIs of the files to be distributed to all workers
   * @param initFn function to initialized the distributed files
   */
  def distCache[F](uris: Seq[String])(initFn: Seq[File] => F): DistCache[F] =
    if (this.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uris.mkString("\t"))))
    } else {
      new DistCacheMulti(uris.map(new URI(_)), initFn, options)
    }

}
