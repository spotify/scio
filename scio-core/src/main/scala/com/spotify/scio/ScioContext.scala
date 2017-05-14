/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// scalastyle:off file.size.limit

package com.spotify.scio

import java.beans.Introspector
import java.io.File
import java.net.{URI, URLClassLoader}
import java.nio.file.Files
import java.util.jar.{Attributes, JarFile}

import com.google.api.services.bigquery.model.TableReference
import com.google.datastore.v1.{Entity, Query}
import com.google.protobuf.Message
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.AvroBytesUtil
import com.spotify.scio.io.{TFRecordOptions, TFRecordSource, Tap}
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.testing._
import com.spotify.scio.util._
import com.spotify.scio.values._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options._
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.extensions.gcp.options.{GcpOptions, GcsOptions}
import org.apache.beam.sdk.io.gcp.{bigquery => bqio, datastore => dsio, pubsub => psio}
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{Create, DoFn, PTransform}
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.{Pipeline, io => gio}
import org.joda.time.Instant
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{Buffer => MBuffer, Map => MMap}
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try

/** Convenience object for creating [[ScioContext]] and [[Args]]. */
object ContextAndArgs {
  /** Create [[ScioContext]] and [[Args]] for command line arguments. */
  def apply(args: Array[String]): (ScioContext, Args) = {
    val (_opts, _args) = ScioContext.parseArguments[PipelineOptions](args)
    (new ScioContext(_opts, Nil), _args)
  }
}

/** Companion object for [[ScioContext]]. */
object ScioContext {

  import org.apache.beam.sdk.options.PipelineOptionsFactory

  /** Create a new [[ScioContext]] instance. */
  def apply(): ScioContext = ScioContext(defaultOptions)

  /** Create a new [[ScioContext]] instance. */
  def apply(options: PipelineOptions): ScioContext = new ScioContext(options,  Nil)

  /** Create a new [[ScioContext]] instance. */
  def apply(artifacts: List[String]): ScioContext = new ScioContext(defaultOptions, artifacts)

  /** Create a new [[ScioContext]] instance. */
  def apply(options: PipelineOptions, artifacts: List[String]): ScioContext =
    new ScioContext(options, artifacts)

  /** Create a new [[ScioContext]] instance for testing. */
  def forTest(): ScioContext = {
    val opts = PipelineOptionsFactory
      .fromArgs("--appName=" + TestUtil.newTestId())
      .as(classOf[PipelineOptions])
    new ScioContext(opts, List[String]())
  }

  /** Parse PipelineOptions and application arguments from command line arguments. */
  def parseArguments[T <: PipelineOptions : ClassTag](cmdlineArgs: Array[String])
  : (T, Args) = {
    val optClass = ScioUtil.classOf[T]

    // Extract --pattern of all registered derived types of PipelineOptions
    val classes = PipelineOptionsFactory.getRegisteredOptions.asScala + optClass
    val optPatterns = classes.flatMap { cls =>
      cls.getMethods.flatMap { m =>
        val n = m.getName
        if ((!n.startsWith("get") && !n.startsWith("is")) ||
          m.getParameterTypes.nonEmpty || m.getReturnType == classOf[Unit]) {
          None
        } else {
          Some(Introspector.decapitalize(n.substring(if (n.startsWith("is")) 2 else 3)))
        }
      }.map(s => s"--$s($$|=)".r)
    }

    // Split cmdlineArgs into 2 parts, optArgs for PipelineOptions and appArgs for Args
    val (optArgs, appArgs) =
      cmdlineArgs.partition(arg => optPatterns.exists(_.findFirstIn(arg).isDefined))

    (PipelineOptionsFactory.fromArgs(optArgs: _*).as(optClass), Args(appArgs))
  }

  import scala.language.implicitConversions

  /** Implicit conversion from ScioContext to DistCacheScioContext. */
  implicit def makeDistCacheScioContext(self: ScioContext): DistCacheScioContext =
    new DistCacheScioContext(self)

  private val defaultOptions: PipelineOptions = PipelineOptionsFactory.create()

}

/**
 * Main entry point for Scio functionality. A ScioContext represents a pipeline and can be used to
 * create SCollections and distributed caches on that cluster.
 *
 * @groupname accumulator Accumulators
 * @groupname dist_cache Distributed Cache
 * @groupname in_memory In-memory Collections
 * @groupname input Input Sources
 * @groupname Ungrouped Other Members
 */
// scalastyle:off number.of.methods
class ScioContext private[scio] (val options: PipelineOptions,
                                 private var artifacts: List[String])
  extends TransformNameable {

  private implicit val context: ScioContext = this

  private val logger = LoggerFactory.getLogger(this.getClass)

  import Implicits._

  /** Get PipelineOptions as a more specific sub-type. */
  def optionsAs[T <: PipelineOptions : ClassTag]: T = options.as(ScioUtil.classOf[T])

  // Set default name if no app name specified by user
  Try(optionsAs[ApplicationNameOptions]).foreach { o =>
    if (o.getAppName == null || o.getAppName.startsWith("ScioContext$")) {
      this.setAppName(CallSites.getAppName)
    }
  }

  // Set default Dataflow job name if none specified by user
  Try(optionsAs[DataflowPipelineOptions]).foreach { o =>
    if (o.getJobName == null) {
      this.setJobName(o.getAppName) // appName already set
    }
  }

  {
    VersionUtil.checkVersion()
    val o = optionsAs[ScioOptions]
    o.setScalaVersion(scalaVersion)
    o.setScioVersion(scioVersion)
  }

  {
    // Check if running within scala.App. See https://github.com/spotify/scio/issues/449
    if (Thread.currentThread()
      .getStackTrace.toList.map(_.getClassName.split('$').head)
      .exists(_.equals(classOf[App].getName))) {
      logger.warn(
        "Applications defined within scala.App might not work properly. Please use main method!")
    }
  }

  private[scio] val testId: Option[String] =
    Try(optionsAs[ApplicationNameOptions]).toOption.flatMap { o =>
      if (TestUtil.isTestId(o.getAppName)) {
        Some(o.getAppName)
      } else {
        None
      }
    }

  /** Underlying pipeline. */
  def pipeline: Pipeline = {
    if (_pipeline == null) {
      // TODO: make sure this works for other PipelineOptions
      Try(optionsAs[DataflowPipelineWorkerPoolOptions])
        .foreach(_.setFilesToStage(getFilesToStage(artifacts).asJava))
      // if in local runner, temp location may be needed, but is not currently required by
      // the runner, which may end up with NPE. If not set but user generate new temp dir
      if (ScioUtil.isLocalRunner(options) && options.getTempLocation == null) {
        val tmpDir = Files.createTempDirectory("scio-temp-")
        logger.debug(s"New temp directory at $tmpDir")
        options.setTempLocation(tmpDir.toString)
      }
      _pipeline = if (testId.isEmpty) {
        Pipeline.create(options)
      } else {
        TestDataManager.startTest(testId.get)
        // load TestPipeline dynamically to avoid ClassNotFoundException when running src/main
        // https://issues.apache.org/jira/browse/BEAM-298
        val cls = Class.forName("org.apache.beam.sdk.testing.TestPipeline")
        // propagate options
        val opts = PipelineOptionsFactory.create()
        opts.setStableUniqueNames(options.getStableUniqueNames)
        val tp = cls.getMethod("create").invoke(null, opts).asInstanceOf[Pipeline]
        // workaround for @Rule enforcement introduced by
        // https://issues.apache.org/jira/browse/BEAM-1205
        cls
          .getMethod("enableAbandonedNodeEnforcement", classOf[Boolean])
          .invoke(tp, Boolean.box(true))
        tp
      }
      _pipeline.getCoderRegistry.registerScalaCoders()
    }
    _pipeline
  }

  /* Mutable members */
  private var _pipeline: Pipeline = _
  private var _isClosed: Boolean = false
  private val _promises: MBuffer[(Promise[Tap[_]], Tap[_])] = MBuffer.empty
  private val _queryJobs: MBuffer[QueryJob] = MBuffer.empty
  private val _accumulators: MMap[String, Accumulator[_]] = MMap.empty
  private val _preRunFns: MBuffer[() => Unit] = MBuffer.empty

  /** Wrap a [[org.apache.beam.sdk.values.PCollection PCollection]]. */
  def wrap[T: ClassTag](p: PCollection[T]): SCollection[T] =
    new SCollectionImpl[T](p, this)

  // =======================================================================
  // Extra artifacts - jars/files etc
  // =======================================================================

  /** Borrowed from DataflowRunner. */
  private def detectClassPathResourcesToStage(classLoader: ClassLoader): List[String] = {
    require(classLoader.isInstanceOf[URLClassLoader],
      "Current ClassLoader is '" + classLoader + "' only URLClassLoaders are supported")

    // exclude jars from JAVA_HOME and files from current directory
    val javaHome = new File(sys.props("java.home")).getCanonicalPath
    val userDir = new File(sys.props("user.dir")).getCanonicalPath

    val classPathJars = classLoader.asInstanceOf[URLClassLoader]
      .getURLs
      .map(url => new File(url.toURI).getCanonicalPath)
      .filter(p => !p.startsWith(javaHome) && p != userDir)
      .toList

    // fetch jars from classpath jar's manifest Class-Path if present
    val manifestJars =  classPathJars
      .filter(_.endsWith(".jar"))
      .map(p => (p, new JarFile(p).getManifest))
      .filter { case (p, manifest) =>
        manifest != null && manifest.getMainAttributes.containsKey(Attributes.Name.CLASS_PATH)}
      .map { case (p, manifest) => (new File(p).getParentFile,
        manifest.getMainAttributes.getValue(Attributes.Name.CLASS_PATH).split(" ")) }
      .flatMap { case (parent, jars) => jars.map(jar =>
        if (jar.startsWith("/")) {
          jar // accept absolute path as is
        } else {
          new File(parent, jar).getCanonicalPath  // relative path
        })
      }

    logger.debug(s"Classpath jars: ${classPathJars.mkString(":")}")
    logger.debug(s"Manifest jars: ${manifestJars.mkString(":")}")

    // no need to care about duplicates here - should be solved by the SDK uploader
    classPathJars ++ manifestJars
  }

  /** Compute list of local files to make available to workers. */
  private def getFilesToStage(extraLocalArtifacts: List[String]): List[String] = {
    val finalLocalArtifacts = detectClassPathResourcesToStage(
      classOf[DataflowRunner].getClassLoader) ++ extraLocalArtifacts

    logger.debug(s"Final list of extra artifacts: ${finalLocalArtifacts.mkString(":")}")
    finalLocalArtifacts
  }

  /**
   * Add artifact to stage in workers. Artifact can be jar/text-files etc.
   * NOTE: currently one can only add artifacts before pipeline object is created.
   */
  def addArtifacts(extraLocalArtifacts: List[String]): Unit = {
    require(_pipeline == null, "Cannot add artifacts once pipeline is initialized")
    artifacts ++= extraLocalArtifacts
  }

  // =======================================================================
  // Miscellaneous
  // =======================================================================

  private lazy val bigQueryClient: BigQueryClient = {
    val o = optionsAs[GcpOptions]
    BigQueryClient(o.getProject, o.getGcpCredential)
  }

  // =======================================================================
  // States
  // =======================================================================

  /** Set application name for the context. */
  def setAppName(name: String): Unit = {
    if (_pipeline != null) {
      throw new RuntimeException("Cannot set application name once pipeline is initialized")
    }
    Try(optionsAs[ApplicationNameOptions]).foreach(_.setAppName(name))
  }

  /** Set job name for the context (Dataflow only) */
  def setJobName(name: String): Unit = {
    if (_pipeline != null) {
      throw new RuntimeException("Cannot set job name once pipeline is initialized")
    }
    Try(optionsAs[DataflowPipelineOptions]).foreach(_.setJobName(name))
  }

  /** Close the context. No operation can be performed once the context is closed. */
  def close(): ScioResult = requireNotClosed {
    if (_queryJobs.nonEmpty) {
      bigQueryClient.waitForJobs(_queryJobs: _*)
    }

    _isClosed = true

    _preRunFns.foreach(_())
    val result = new ScioResult(this.pipeline.run(), _accumulators.values.toSeq, context)

    if (this.isTest) {
      TestDataManager.closeTest(testId.get)
    }

    if (this.isTest || this.optionsAs[ScioOptions].isBlocking) {
      result.waitUntilDone()  // block local runner for JobTest to work
    } else {
      result
    }
  }

  /** Whether the context is closed. */
  def isClosed: Boolean = _isClosed

  /** Ensure an operation is called before the pipeline is closed. */
  private[scio] def requireNotClosed[T](body: => T): T = {
    require(!this.isClosed, "ScioContext already closed")
    body
  }

  // =======================================================================
  // Futures
  // =======================================================================

  // To be updated once the pipeline completes.
  private[scio] def makeFuture[T](value: Tap[T]): Future[Tap[T]] = {
    val p = Promise[Tap[T]]()
    _promises.append((p.asInstanceOf[Promise[Tap[_]]], value.asInstanceOf[Tap[_]]))
    p.future
  }

  // Update pending futures after pipeline completes.
  private[scio] def updateFutures(state: State): Unit = _promises.foreach { kv =>
    if (state == State.DONE || state == State.UPDATED) {
      kv._1.success(kv._2)
    } else {
      kv._1.failure(new RuntimeException("Pipeline failed to complete: " + state))
    }
  }

  // =======================================================================
  // Test wiring
  // =======================================================================

  /**  Whether this is a test context. */
  def isTest: Boolean = testId.isDefined

  private[scio] def testIn: TestInput = TestDataManager.getInput(testId.get)
  private[scio] def testOut: TestOutput = TestDataManager.getOutput(testId.get)
  private[scio] def testDistCache: TestDistCache = TestDataManager.getDistCache(testId.get)

  private[scio] def getTestInput[T: ClassTag](key: TestIO[T]): SCollection[T] =
    this.parallelize(testIn(key).asInstanceOf[Seq[T]])

  // =======================================================================
  // Read operations
  // =======================================================================

  private[scio] def applyInternal[Output <: POutput](root: PTransform[_ >: PBegin, Output])
  : Output =
    pipeline.apply(this.tfName, root)

  /**
   * Get an SCollection for an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   * @group input
   */
  def objectFile[T: ClassTag](path: String): SCollection[T] = requireNotClosed {
    if (this.isTest) {
      this.getTestInput(ObjectFileIO[T](path))
    } else {
      val coder = pipeline.getCoderRegistry.getScalaCoder[T]
      this.avroFile[GenericRecord](path, AvroBytesUtil.schema)
        .parDo(new DoFn[GenericRecord, T] {
          @ProcessElement
          private[scio] def processElement(c: DoFn[GenericRecord, T]#ProcessContext): Unit = {
            c.output(AvroBytesUtil.decode(coder, c.element()))
          }
        })
        .setName(path)
    }
  }

  /**
   * Get an SCollection for an Avro file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @group input
   */
  def avroFile[T: ClassTag](path: String, schema: Schema = null): SCollection[T] =
  requireNotClosed {
    if (this.isTest) {
      this.getTestInput(AvroIO[T](path))
    } else {
      val cls = ScioUtil.classOf[T]
      val t = if (classOf[GenericRecord] isAssignableFrom cls) {
        gio.AvroIO.readGenericRecords(schema).from(path).asInstanceOf[gio.AvroIO.Read[T]]
      } else {
        gio.AvroIO.read(cls).from(path)
      }
      wrap(this.applyInternal(t)).setName(path)
    }
  }

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   * @group input
   */
  def protobufFile[T: ClassTag](path: String)(implicit ev: T <:< Message): SCollection[T] =
    requireNotClosed {
      if (this.isTest) {
        this.getTestInput(ProtobufIO[T](path))
      } else {
        objectFile(path)
      }
    }

  /**
   * Get an SCollection for a BigQuery SELECT query.
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   * @group input
   */
  def bigQuerySelect(sqlQuery: String,
                     flattenResults: Boolean = false): SCollection[TableRow] = requireNotClosed {
    if (this.isTest) {
      this.getTestInput(BigQueryIO(sqlQuery))
    } else if (this.bigQueryClient.isCacheEnabled) {
      val queryJob = this.bigQueryClient.newQueryJob(sqlQuery, flattenResults)
      _queryJobs.append(queryJob)
      wrap(this.applyInternal(bqio.BigQueryIO.read().from(queryJob.table).withoutValidation()))
        .setName(sqlQuery)
    } else {
      val baseQuery = if (!flattenResults) {
        bqio.BigQueryIO.read().fromQuery(sqlQuery).withoutResultFlattening()
      } else {
        bqio.BigQueryIO.read().fromQuery(sqlQuery)
      }
      val query = if (this.bigQueryClient.isLegacySql(sqlQuery, flattenResults)) {
        baseQuery
      } else {
        baseQuery.usingStandardSql()
      }
      wrap(this.applyInternal(query)).setName(sqlQuery)
    }
  }

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(table: TableReference): SCollection[TableRow] = requireNotClosed {
    val tableSpec: String = bqio.BigQueryHelpers.toTableSpec(table)
    if (this.isTest) {
      this.getTestInput(BigQueryIO(tableSpec))
    } else {
      wrap(this.applyInternal(bqio.BigQueryIO.read().from(table))).setName(tableSpec)
    }
  }

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(tableSpec: String): SCollection[TableRow] =
    this.bigQueryTable(bqio.BigQueryHelpers.parseTableSpec(tableSpec))

  /**
   * Get a typed SCollection for a BigQuery SELECT query or table.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromSchema]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]], or
   * [[com.spotify.scio.bigquery.types.BigQueryType.toTable BigQueryType.toTable]].
   *
   * By default the source (table or query) specified in the annotation will be used, but it can
   * be overridden with the `newSource` parameter. For example:
   *
   * {{{
   * @BigQueryType.fromTable("publicdata:samples.gsod")
   * class Row
   *
   * // Read from [publicdata:samples.gsod] as specified in the annotation.
   * sc.typedBigQuery[Row]()
   *
   * // Read from [myproject:samples.gsod] instead.
   * sc.typedBigQuery[Row]("myproject:samples.gsod")
   *
   * // Read from a query instead.
   * sc.typedBigQuery[Row]("SELECT * FROM [publicdata:samples.gsod] LIMIT 1000")
   * }}}
   *
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def typedBigQuery[T <: HasAnnotation : ClassTag : TypeTag](newSource: String = null)
  : SCollection[T] = {
    val bqt = BigQueryType[T]
    val rows = if (newSource == null) {
      // newSource is missing, T's companion object must have either table or query
      if (bqt.isTable) {
        this.bigQueryTable(bqt.table.get)
      } else if (bqt.isQuery) {
        this.bigQuerySelect(bqt.query.get)
      } else {
        throw new IllegalArgumentException(s"Missing table or query field in companion object")
      }
    } else {
      // newSource can be either table or query
      val table = scala.util.Try(bqio.BigQueryHelpers.parseTableSpec(newSource)).toOption
      if (table.isDefined) {
        this.bigQueryTable(table.get)
      } else {
        this.bigQuerySelect(newSource)
      }
    }
    rows.map(bqt.fromTableRow)
  }

  /**
   * Get an SCollection for a Datastore query.
   * @group input
   */
  def datastore(projectId: String, query: Query, namespace: String = null): SCollection[Entity] =
    requireNotClosed {
      if (this.isTest) {
        this.getTestInput(DatastoreIO(projectId, query, namespace))
      } else {
        wrap(this.applyInternal(
          dsio.DatastoreIO.v1().read()
            .withProjectId(projectId)
            .withNamespace(namespace)
            .withQuery(query)))
      }
    }

  private def pubsubIn[T: ClassTag](isSubscription: Boolean,
                                    name: String,
                                    idAttribute: String,
                                    timestampAttribute: String)
  : SCollection[T] = requireNotClosed {
    if (this.isTest) {
      this.getTestInput(PubsubIO(name))
    } else {
      val cls = ScioUtil.classOf[T]
      def setup[U](read: psio.PubsubIO.Read[U]) = {
        var r = read
        r = if (isSubscription) r.fromSubscription(name) else r.fromTopic(name)
        if (idAttribute != null) {
          r = r.withIdAttribute(idAttribute)
        }
        if (timestampAttribute != null) {
          r = r.withTimestampAttribute(timestampAttribute)
        }
        r
      }
      if (classOf[String] isAssignableFrom cls) {
        val t = setup(psio.PubsubIO.readStrings())
        wrap(this.applyInternal(t)).setName(name).asInstanceOf[SCollection[T]]
      } else if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        val t = setup(psio.PubsubIO.readAvros(cls))
        wrap(this.applyInternal(t)).setName(name)
      } else if (classOf[Message] isAssignableFrom cls) {
        val t = setup(psio.PubsubIO.readProtos(cls))
        wrap(this.applyInternal(t)).setName(name)
      } else {
        val coder = pipeline.getCoderRegistry.getScalaCoder[T]
        val t = setup(psio.PubsubIO.readMessages())
        wrap(this.applyInternal(t)).setName(name)
          .map(m => CoderUtils.decodeFromByteArray(coder, m.getPayload))
      }
    }
  }

  /**
   * Get an SCollection for a Pub/Sub subscription.
   * @group input
   */
  def pubsubSubscription[T: ClassTag](sub: String,
                                      idAttribute: String = null,
                                      timestampAttribute: String = null)
  : SCollection[T] = pubsubIn(isSubscription = true, sub, idAttribute, timestampAttribute)

  /**
    * Get an SCollection for a Pub/Sub topic.
    * @group input
    */
  def pubsubTopic[T: ClassTag](topic: String,
                               idAttribute: String = null,
                               timestampAttribute: String = null)
  : SCollection[T] = pubsubIn(isSubscription = false, topic, idAttribute, timestampAttribute)

  private def pubsubInWithAttributes[T: ClassTag](isSubscription: Boolean,
                                                  name: String,
                                                  idAttribute: String,
                                                  timestampAttribute: String)
  : SCollection[(T, Map[String, String])] = requireNotClosed {
    if (this.isTest) {
      this.getTestInput(PubsubIO(name))
    } else {
      var t = psio.PubsubIO.readMessagesWithAttributes()
      t = if (isSubscription) t.fromSubscription(name) else t.fromTopic(name)
      if (idAttribute != null) {
        t = t.withIdAttribute(idAttribute)
      }
      if (timestampAttribute != null) {
        t = t.withTimestampAttribute(timestampAttribute)
      }
      val elementCoder = pipeline.getCoderRegistry.getScalaCoder[T]
      wrap(this.applyInternal(t)).setName(name)
        .map { m =>
          val payload = CoderUtils.decodeFromByteArray(elementCoder, m.getPayload)
          val attributes = JMapWrapper.of(m.getAttributeMap)
          (payload, attributes)
        }
    }
  }

  /**
    * Get an SCollection for a Pub/Sub subscription that includes message attributes.
    * @group input
    */
  def pubsubSubscriptionWithAttributes[T: ClassTag](sub: String,
                                                    idAttribute: String = null,
                                                    timestampAttribute: String = null)
  : SCollection[(T, Map[String, String])] =
    pubsubInWithAttributes(isSubscription = true, sub, idAttribute, timestampAttribute)

  /**
    * Get an SCollection for a Pub/Sub topic that includes message attributes.
    * @group input
    */
  def pubsubTopicWithAttributes[T: ClassTag](topic: String,
                               idLabel: String = null,
                               timestampLabel: String = null)
  : SCollection[(T, Map[String, String])] =
    pubsubInWithAttributes(isSubscription = false, topic, idLabel, timestampLabel)

  /**
   * Get an SCollection for a BigQuery TableRow JSON file.
   * @group input
   */
  def tableRowJsonFile(path: String): SCollection[TableRow] = requireNotClosed {
    if (this.isTest) {
      this.getTestInput[TableRow](TableRowJsonIO(path))
    } else {
      wrap(this.applyInternal(gio.TextIO.read().from(path))).setName(path)
        .map(e => ScioUtil.jsonFactory.fromString(e, classOf[TableRow]))
    }
  }

  /**
   * Get an SCollection for a text file.
   * @group input
   */
  def textFile(path: String,
               compressionType: gio.TextIO.CompressionType = gio.TextIO.CompressionType.AUTO)
  : SCollection[String] = requireNotClosed {
    if (this.isTest) {
      this.getTestInput(TextIO(path))
    } else {
      wrap(this.applyInternal(gio.TextIO.read().from(path)
        .withCompressionType(compressionType))).setName(path)
    }
  }

  /**
   * Get an SCollection for a TensorFlow TFRecord file. Note that TFRecord files are not
   * splittable. The recommended record encoding is [[org.tensorflow.example.Example]] protocol
   * buffers (which contain [[org.tensorflow.example.Features]] as a field) serialized as bytes.
   * @group input
   */
  def tfRecordFile(path: String, tfRecordOptions: TFRecordOptions = TFRecordOptions.readDefault)
  : SCollection[Array[Byte]] = requireNotClosed {
    if (this.isTest) {
      this.getTestInput(TFRecordIO(path))
    } else {
      wrap(this.applyInternal(gio.Read.from(TFRecordSource(path, tfRecordOptions))))
    }
  }

  /**
   * Get an SCollection with a custom input transform. The transform should have a unique name.
   * @group input
   */
  def customInput[T : ClassTag](name: String, transform: PTransform[PBegin, PCollection[T]])
  : SCollection[T] = requireNotClosed {
    if (this.isTest) {
      this.getTestInput(CustomIO[T](name))
    } else {
      wrap(this.pipeline.apply(name, transform))
    }
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
  def maxAccumulator[T](n: String)(implicit at: AccumulatorType[T]): Accumulator[T] =
    makeAccumulator(n, at.maxFn())

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the minimum value. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def minAccumulator[T](n: String)(implicit at: AccumulatorType[T]): Accumulator[T] =
    makeAccumulator(n, at.minFn())

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the sum of values. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def sumAccumulator[T](n: String)(implicit at: AccumulatorType[T]): Accumulator[T] =
    makeAccumulator(n, at.sumFn())

  private def makeAccumulator[T](n: String, fn: CombineFn[T, _, T]) = requireNotClosed {
    require(!_accumulators.contains(n), s"Accumulator '$n' already exists")
    val acc = new Accumulator[T] {
      override val name: String = n
      override val combineFn: CombineFn[T, _, T] = fn
    }
    _accumulators.put(n, acc)
    acc
  }

  private[scio] def containsAccumulator(acc: Accumulator[_]): Boolean =
    _accumulators.contains(acc.name)

  private[scio] def addPreRunFn(f: () => Unit): Unit = _preRunFns += f

  // =======================================================================
  // In-memory collections
  // =======================================================================

  private def truncate(name: String): String = {
    val maxLength = 256
    if (name.length <= maxLength) name else name.substring(0, maxLength - 3) + "..."
  }

  /**
   * Distribute a local Scala `Iterable` to form an SCollection.
   * @group in_memory
   */
  def parallelize[T: ClassTag](elems: Iterable[T]): SCollection[T] = requireNotClosed {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    wrap(this.applyInternal(Create.of(elems.asJava).withCoder(coder)))
      .setName(truncate(elems.toString()))
  }

  /**
   * Distribute a local Scala `Map` to form an SCollection.
   * @group in_memory
   */
  def parallelize[K: ClassTag, V: ClassTag](elems: Map[K, V]): SCollection[(K, V)] =
  requireNotClosed {
    val coder = pipeline.getCoderRegistry.getScalaKvCoder[K, V]
    wrap(this.applyInternal(Create.of(elems.asJava).withCoder(coder)))
      .map(kv => (kv.getKey, kv.getValue))
      .setName(truncate(elems.toString()))
  }

  /**
   * Distribute a local Scala `Iterable` with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[(T, Instant)])
  : SCollection[T] = requireNotClosed {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.map(t => TimestampedValue.of(t._1, t._2))
    wrap(this.applyInternal(Create.timestamped(v.asJava).withCoder(coder)))
      .setName(truncate(elems.toString()))
  }

  /**
   * Distribute a local Scala `Iterable` with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[T], timestamps: Iterable[Instant])
  : SCollection[T] = requireNotClosed {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.zip(timestamps).map(t => TimestampedValue.of(t._1, t._2))
    wrap(this.applyInternal(Create.timestamped(v.asJava).withCoder(coder)))
      .setName(truncate(elems.toString()))
  }

}
// scalastyle:on number.of.methods

/** An enhanced ScioContext with distributed cache features. */
class DistCacheScioContext private[scio] (self: ScioContext) {

  private[scio] def testDistCache: TestDistCache = TestDataManager.getDistCache(self.testId.get)

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
  def distCache[F](uri: String)(initFn: File => F): DistCache[F] = self.requireNotClosed {
    if (self.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uri)))
    } else {
      new DistCacheSingle(new URI(uri), initFn, self.optionsAs[GcsOptions])
    }
  }

  /**
   * Create a new [[com.spotify.scio.values.DistCache DistCache]] instance.
   * @param uris Google Cloud Storage URIs of the files to be distributed to all workers
   * @param initFn function to initialized the distributed files
   * @group dist_cache
   */
  def distCache[F](uris: Seq[String])(initFn: Seq[File] => F): DistCache[F] =
    self.requireNotClosed {
      if (self.isTest) {
        new MockDistCache(testDistCache(DistCacheIO(uris)))
      } else {
        new DistCacheMulti(uris.map(new URI(_)), initFn, self.optionsAs[GcsOptions])
      }
    }

}

// scalastyle:on file.size.limit
