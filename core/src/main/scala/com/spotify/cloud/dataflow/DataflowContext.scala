package com.spotify.cloud.dataflow

import java.beans.Introspector
import java.io.File
import java.net.URI

import com.google.api.services.bigquery.model._
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
import com.spotify.cloud.bigquery.BigQueryClient
import com.spotify.cloud.dataflow.testing._
import com.spotify.cloud.dataflow.util.CallSites
import com.spotify.cloud.dataflow.values._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.joda.time.Instant

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object ContextAndArgs {
  def apply(args: Array[String]): (DataflowContext, Args) = {
    val context = DataflowContext(args)
    (context, context.args)
  }
}

object DataflowContext {
  def apply(args: Array[String]): DataflowContext = new DataflowContext(args)
}

class DataflowContext private (cmdlineArgs: Array[String]) extends PrivateImplicits {

  private var _options: DataflowPipelineOptions = null
  def options: DataflowPipelineOptions = _options

  private var _sqlId: Int = 0

  val (args, pipeline) = {
    val dfPatterns = classOf[DataflowPipelineOptions].getMethods.flatMap { m =>
      val n = m.getName
      if ((!n.startsWith("get") && !n.startsWith("is")) ||
        m.getParameterTypes.length != 0 || m.getReturnType == classOf[Unit]) {
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

  private lazy val bigQueryClient: BigQueryClient = BigQueryClient(this.options.getGcpCredential)

  def close(): Unit = pipeline.run()

  /* Test wiring */

  private implicit def context: DataflowContext = this

  private[dataflow] def isTest: Boolean = args.optional("testId").isDefined

  private[dataflow] def testIn: TestInput = TestDataManager.getInput(args("testId"))
  private[dataflow] def testOut: TestOutput = TestDataManager.getOutput(args("testId"))
  private[dataflow] def testDistCache: TestDistCache = TestDataManager.getDistCache(args("testId"))

  private def getTestInput[T: ClassTag](key: TestIO[T]): SCollection[T] =
    this.parallelize(testIn(key).asInstanceOf[Seq[T]])

  /* Read operations */

  private def applyInternal[Output <: POutput](root: PTransform[_ >: PBegin, Output]): Output =
    pipeline.apply(root.withName(CallSites.getCurrent))

  def avroFile[T <: IndexedRecord: ClassTag](path: String): SCollection[T] =
    if (this.isTest) {
      this.getTestInput(AvroIO[T](path))
    } else {
      val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
      val o = this.applyInternal(GAvroIO.Read.from(path).withSchema(cls))
      SCollection(o).setName(path)
    }

  def avroFile(path: String, schema: Schema): SCollection[GenericRecord] =
    if (this.isTest) {
      this.getTestInput(AvroIO[GenericRecord](path))
    } else {
      SCollection(this.applyInternal(GAvroIO.Read.from(path).withSchema(schema))).setName(path)
    }

  def avroFile(path: String, schemaString: String): SCollection[GenericRecord] =
    if (this.isTest) {
      this.getTestInput(AvroIO[GenericRecord](path))
    } else {
      SCollection(this.applyInternal(GAvroIO.Read.from(path).withSchema(schemaString))).setName(path)
    }

  def bigQuerySelect(sqlQuery: String, stagingDataset: String): SCollection[TableRow] =
    if (this.isTest) {
      this.getTestInput(BigQueryIO(sqlQuery))
    } else {
      val tableId = this.options.getJobName.replaceAll("-", "_") + "_" + _sqlId
      _sqlId += 1

      val table = new TableReference()
        .setProjectId(options.getProject)
        .setDatasetId(stagingDataset)
        .setTableId(tableId)

      this.bigQueryClient.queryIntoTable(sqlQuery, table)
      bigQueryTable(table).setName(sqlQuery)
    }

  def bigQueryTable(table: TableReference): SCollection[TableRow] = {
    val tableSpec: String = GBigQueryIO.toTableSpec(table)
    if (this.isTest) {
      this.getTestInput(BigQueryIO(tableSpec))
    } else {
      SCollection(this.applyInternal(GBigQueryIO.Read.from(table))).setName(tableSpec)
    }
  }

  def bigQueryTable(tableSpec: String): SCollection[TableRow] =
    this.bigQueryTable(GBigQueryIO.parseTableSpec(tableSpec))

  def datastore(datasetId: String, query: Query): SCollection[Entity] =
    if (this.isTest) {
      this.getTestInput(DatastoreIO(datasetId, query))
    } else {
      SCollection(this.applyInternal(GDatastoreIO.readFrom(datasetId, query)))
    }

  def pubsubSubscription(subscription: String, dropLateData: Boolean = true): SCollection[String] =
    if (this.isTest) {
      this.getTestInput(PubsubIO(subscription))
    } else {
      SCollection(this.applyInternal(GPubsubIO.Read.subscription(subscription).dropLateData(dropLateData)))
        .setName(subscription)
    }

  def pubsubTopic(topic: String, dropLateData: Boolean = true): SCollection[String] =
    if (this.isTest) {
      this.getTestInput(PubsubIO(topic))
    } else {
      SCollection(this.applyInternal(GPubsubIO.Read.topic(topic).dropLateData(dropLateData))).setName(topic)
    }

  def tableRowJsonFile(path: String): SCollection[TableRow] =
    if (this.isTest) {
      this.getTestInput(TableRowJsonIO(path))
    } else {
      SCollection(this.applyInternal(GTextIO.Read.from(path).withCoder(TableRowJsonCoder.of()))).setName(path)
    }

  def textFile[T](path: String): SCollection[String] =
    if (this.isTest) {
      this.getTestInput(TextIO(path))
    } else {
      SCollection(this.applyInternal(GTextIO.Read.from(path))).setName(path)
    }

  /* In-memory collections */

  def parallelize[T: ClassTag](elems: T*): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    SCollection(this.applyInternal(Create.of(elems: _*)).setCoder(coder)).setName(elems.toString())
  }

  def parallelize[T: ClassTag](elems: Iterable[T]): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    SCollection(this.applyInternal(Create.of(elems.asJava)).setCoder(coder)).setName(elems.toString())
  }

  def parallelize[K: ClassTag, V: ClassTag](elems: Map[K, V]): SCollection[(K, V)] = {
    val coder = pipeline.getCoderRegistry.getScalaKvCoder[K, V]
    SCollection(this.applyInternal(Create.of(elems.asJava)).setCoder(coder)).map(kv => (kv.getKey, kv.getValue))
      .setName(elems.toString())
  }

  def parallelizeTimestamped[T: ClassTag](elems: (T, Instant)*): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.map(t => TimestampedValue.of(t._1, t._2))
    SCollection(this.applyInternal(Create.timestamped(v: _*)).setCoder(coder)).setName(elems.toString())
  }

  def parallelizeTimestamped[T: ClassTag](elems: Iterable[(T, Instant)]): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.map(t => TimestampedValue.of(t._1, t._2))
    SCollection(this.applyInternal(Create.timestamped(v.asJava)).setCoder(coder)).setName(elems.toString())
  }

  def parallelizeTimestamped[T: ClassTag](elems: Iterable[T],
                                          timestamps: Iterable[Instant]): SCollection[T] = {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.zip(timestamps).map(t => TimestampedValue.of(t._1, t._2))
    SCollection(this.applyInternal(Create.timestamped(v.asJava)).setCoder(coder)).setName(elems.toString())
  }

  /* Distributed cache */

  def distCache[F](uri: String)(initFn: File => F): DistCache[F] =
    if (this.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uri)))
    } else {
      new DistCacheSingle(new URI(uri), initFn, options)
    }

  def distCache[F](uris: Seq[String])(initFn: Seq[File] => F): DistCache[F] =
    if (this.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uris.mkString("\t"))))
    } else {
      new DistCacheMulti(uris.map(new URI(_)), initFn, options)
    }

}
