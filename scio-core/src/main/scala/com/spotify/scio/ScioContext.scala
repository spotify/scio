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

package com.spotify.scio

import java.beans.Introspector
import java.io.File
import java.net.{URI, URLClassLoader}
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.jar.{Attributes, JarFile}

import com.google.datastore.v1.{Query, Entity}
import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder
import com.google.cloud.dataflow.sdk.io.PatchedAvroIO
import com.google.cloud.dataflow.sdk.{io => gio}
import com.google.cloud.dataflow.sdk.options._
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.google.cloud.dataflow.sdk.runners.{DataflowPipelineJob, DataflowPipelineRunner}
import com.google.cloud.dataflow.sdk.testing.TestPipeline
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn
import com.google.cloud.dataflow.sdk.transforms.{Create, DoFn, PTransform}
import com.google.cloud.dataflow.sdk.values.{PBegin, PCollection, POutput, TimestampedValue}
import com.google.protobuf.Message
import com.spotify.scio.bigquery._
import com.spotify.scio.coders.AvroBytesUtil
import com.spotify.scio.io.Tap
import com.spotify.scio.testing._
import com.spotify.scio.util.{CallSites, ScioUtil}
import com.spotify.scio.values._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.joda.time.Instant
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{Buffer => MBuffer, Map => MMap}
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal

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

  import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory

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
      .fromArgs(Array("--appName=" + JobTest.newTestId()))
      .as(classOf[ApplicationNameOptions])

    opts.setRunner(classOf[InProcessPipelineRunner])
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

    (PipelineOptionsFactory.fromArgs(optArgs).as(optClass), Args(appArgs))
  }

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
                                 private var artifacts: List[String]) {

  private implicit val context: ScioContext = this

  private val logger = LoggerFactory.getLogger(ScioContext.getClass)

  import Implicits._

  /** Get PipelineOptions as a more specific sub-type. */
  def optionsAs[T <: PipelineOptions : ClassTag]: T = options.as(ScioUtil.classOf[T])

  // Set default name if no app name specified by user
  Try(optionsAs[ApplicationNameOptions]).foreach { o =>
    if (o.getAppName == null || o.getAppName.startsWith("ScioContext$")) {
      this.setName(CallSites.getAppName)
    }
  }

  private[scio] val testId: Option[String] =
    Try(optionsAs[ApplicationNameOptions]).toOption.flatMap { o =>
      if (JobTest.isTestId(o.getAppName)) {
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
      _pipeline = if (testId.isEmpty) {
        // if in local runner, temp location may be needed, but is not currently required by
        // the runner, which may end up with NPE. If not set but user generate new temp dir
        if (ScioUtil.isLocalRunner(options) && options.getTempLocation == null) {
          val tmpDir = Files.createTempDirectory("scio-temp-")
          logger.debug(s"New temp directory at $tmpDir")
          options.setTempLocation(tmpDir.toString)
        }
        Pipeline.create(options)
      } else {
        val testOpts = TestPipeline.testingPipelineOptions()
        // propagate options
        testOpts.setRunner(options.getRunner)
        testOpts.setStableUniqueNames(options.getStableUniqueNames)
        TestPipeline.fromOptions(testOpts)
      }
      _pipeline.getCoderRegistry.registerScalaCoders()
    }
    _pipeline
  }

  /* Mutable members */
  private var _pipeline: Pipeline = null
  private var _isClosed: Boolean = false
  private val _promises: MBuffer[(Promise[Tap[_]], Tap[_])] = MBuffer.empty
  private val _queryJobs: MBuffer[QueryJob] = MBuffer.empty
  private val _accumulators: MMap[String, Accumulator[_]] = MMap.empty

  /** Wrap a [[com.google.cloud.dataflow.sdk.values.PCollection PCollection]]. */
  def wrap[T: ClassTag](p: PCollection[T]): SCollection[T] =
    new SCollectionImpl[T](p, this)

  // =======================================================================
  // Extra artifacts - jars/files etc
  // =======================================================================

  /** Borrowed from DataflowPipelineRunner. */
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
      classOf[DataflowPipelineRunner].getClassLoader) ++ extraLocalArtifacts

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

  /** Set name for the context. */
  def setName(name: String): Unit = {
    if (_pipeline != null) {
      throw new RuntimeException("Cannot set name once pipeline is initialized")
    }
    // override app name and job name
    Try(optionsAs[ApplicationNameOptions]).foreach(_.setAppName(name))
    Try(optionsAs[DataflowPipelineOptions])
      .foreach(_.setJobName(new DataflowPipelineOptions.JobNameFactory().create(options)))
  }

  /** Close the context. No operation can be performed once the context is closed. */
  def close(): ScioResult = {
    if (_queryJobs.nonEmpty) {
      bigQueryClient.waitForJobs(_queryJobs: _*)
    }

    _isClosed = true
    val result = this.pipeline.run()

    val finalState = result match {
      // non-blocking runner, handle callbacks asynchronously
      case job: DataflowPipelineJob =>
        import scala.concurrent.ExecutionContext.Implicits.global
        val f = Future {
          val state = job.waitToFinish(-1, TimeUnit.SECONDS, null)
          updateFutures(state)
          state
        }
        f.onFailure {
          case NonFatal(e) => _promises.foreach(_._1.failure(e))
        }
        f
      // blocking runner, handle callbacks directly
      case _ =>
        updateFutures(result.getState)
        Future.successful(result.getState)
    }

    new ScioResult(result, finalState, _accumulators.values.toSeq, pipeline)
  }

  /** Whether the context is closed. */
  def isClosed: Boolean = _isClosed

  /** Ensure an operation is called before the pipeline is closed. */
  private[scio] def pipelineOp[T](body: => T): T = {
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

  private def updateFutures(state: State): Unit = _promises.foreach { kv =>
    if (state == State.DONE || state == State.UPDATED) {
      kv._1.success(kv._2)
    } else {
      kv._1.failure(new RuntimeException("Pipeline failed to complete: " + state))
    }
  }

  // =======================================================================
  // Test wiring
  // =======================================================================

  private[scio] def isTest: Boolean = testId.isDefined

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
    pipeline.apply(CallSites.getCurrent, root)

  /**
   * Apply a [[com.google.cloud.dataflow.sdk.transforms.PTransform PTransform]] and wrap the output
   * in an [[SCollection]].
   */
  def applyTransform[T: ClassTag](root: PTransform[_ >: PBegin, PCollection[T]]): SCollection[T] =
    this.wrap(this.applyInternal(root))

  /**
   * Get an SCollection for an object file.
   * @group input
   */
  def objectFile[T: ClassTag](path: String): SCollection[T] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(ObjectFileIO[T](path))
    } else {
      val coder = pipeline.getCoderRegistry.getScalaCoder[T]
      this.avroFile[GenericRecord](path, AvroBytesUtil.schema)
        .parDo(new DoFn[GenericRecord, T] {
          override def processElement(c: DoFn[GenericRecord, T]#ProcessContext): Unit = {
            c.output(AvroBytesUtil.decode(coder, c.element()))
          }
        })
        .setName(path)
    }
  }

  /**
   * Get an SCollection for an Avro file.
   * @group input
   */
  def avroFile[T: ClassTag](path: String, schema: Schema = null): SCollection[T] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(AvroIO[T](path))
    } else {
      val transform = gio.PatchedAvroIO.Read.from(path)
      val cls = ScioUtil.classOf[T]
      val t = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        transform.withSchema(cls)
      } else {
        transform.withSchema(schema).asInstanceOf[PatchedAvroIO.Read.Bound[T]]
      }
      wrap(this.applyInternal(t)).setName(path)
    }
  }

  /**
   * Get an SCollection for a Protobuf file.
   * @group input
   */
  def protobufFile[T: ClassTag](path: String)(implicit ev: T <:< Message): SCollection[T] =
    objectFile(path)

  /**
   * Get an SCollection for a BigQuery SELECT query.
   * @group input
   */
  def bigQuerySelect(sqlQuery: String,
                     flattenResults: Boolean = false): SCollection[TableRow] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(BigQueryIO(sqlQuery))
    } else {
      val queryJob = this.bigQueryClient.newQueryJob(sqlQuery, flattenResults)
      _queryJobs.append(queryJob)
      wrap(this.applyInternal(gio.BigQueryIO.Read.from(queryJob.table).withoutValidation()))
        .setName(sqlQuery)
    }
  }

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(table: TableReference): SCollection[TableRow] = pipelineOp {
    val tableSpec: String = gio.BigQueryIO.toTableSpec(table)
    if (this.isTest) {
      this.getTestInput(BigQueryIO(tableSpec))
    } else {
      wrap(this.applyInternal(gio.BigQueryIO.Read.from(table))).setName(tableSpec)
    }
  }

  /**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
  def bigQueryTable(tableSpec: String): SCollection[TableRow] =
    this.bigQueryTable(gio.BigQueryIO.parseTableSpec(tableSpec))

  /**
   * Get an SCollection for a Datastore query.
   * @group input
   */
  def datastore(projectId: String, query: Query, namespace: String = null): SCollection[Entity] =
    pipelineOp {
      if (this.isTest) {
        this.getTestInput(DatastoreIO(projectId, query, namespace))
      } else {
        wrap(this.applyInternal(
          gio.datastore.DatastoreIO.v1().read()
            .withProjectId(projectId)
            .withNamespace(namespace)
            .withQuery(query)))
      }
    }

  /**
   * Get an SCollection for a Pub/Sub subscription.
   * @group input
   */
  def pubsubSubscription(sub: String,
                         idLabel: String = null,
                         timestampLabel: String = null): SCollection[String] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(PubsubIO(sub))
    } else {
      var transform = gio.PubsubIO.Read.subscription(sub)
      if (idLabel != null) {
        transform = transform.idLabel(idLabel)
      }
      if (timestampLabel != null) {
        transform = transform.timestampLabel(timestampLabel)
      }
      wrap(this.applyInternal(transform)).setName(sub)
    }
  }

  /**
   * Get an SCollection for a Pub/Sub topic.
   * @group input
   */
  def pubsubTopic(topic: String,
                  idLabel: String = null,
                  timestampLabel: String = null): SCollection[String] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(PubsubIO(topic))
    } else {
      var transform = gio.PubsubIO.Read.topic(topic)
      if (idLabel != null) {
        transform = transform.idLabel(idLabel)
      }
      if (timestampLabel != null) {
        transform = transform.timestampLabel(timestampLabel)
      }
      wrap(this.applyInternal(transform)).setName(topic)
    }
  }

  /**
   * Get an SCollection of TableRow for a JSON file.
   * @group input
   */
  def tableRowJsonFile(path: String): SCollection[TableRow] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(TableRowJsonIO(path))
    } else {
      wrap(this.applyInternal(gio.TextIO.Read.from(path).withCoder(TableRowJsonCoder.of())))
        .setName(path)
    }
  }

  /**
   * Get an SCollection for a text file.
   * @group input
   */
  def textFile(path: String,
               compressionType: gio.TextIO.CompressionType = gio.TextIO.CompressionType.AUTO)
  : SCollection[String] = pipelineOp {
    if (this.isTest) {
      this.getTestInput(TextIO(path))
    } else {
      wrap(this.applyInternal(gio.TextIO.Read.from(path)
        .withCompressionType(compressionType))).setName(path)
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
  def maxAccumulator[T](n: String)(implicit at: AccumulatorType[T]): Accumulator[T] = pipelineOp {
    require(!_accumulators.contains(n), s"Accumulator '$n' already exists")
    val acc = new Accumulator[T] {
      override val name: String = n
      override val combineFn: CombineFn[T, _, T] = at.maxFn()
    }
    _accumulators.put(n, acc)
    acc
  }

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the minimum value. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def minAccumulator[T](n: String)(implicit at: AccumulatorType[T]): Accumulator[T] = pipelineOp {
    require(!_accumulators.contains(n), s"Accumulator '$n' already exists")
    val acc = new Accumulator[T] {
      override val name: String = n
      override val combineFn: CombineFn[T, _, T] = at.minFn()
    }
    _accumulators.put(n, acc)
    acc
  }

  /**
   * Create a new [[com.spotify.scio.values.Accumulator Accumulator]] that keeps track
   * of the sum of values. See
   * [[com.spotify.scio.values.SCollection.withAccumulator SCollection.withAccumulator]]
   * for examples.
   * @group accumulator
   */
  def sumAccumulator[T](n: String)(implicit at: AccumulatorType[T]): Accumulator[T] = pipelineOp {
    require(!_accumulators.contains(n), s"Accumulator '$n' already exists")
    val acc = new Accumulator[T] {
      override val name: String = n
      override val combineFn: CombineFn[T, _, T] = at.sumFn()
    }
    _accumulators.put(n, acc)
    acc
  }

  // =======================================================================
  // In-memory collections
  // =======================================================================

  private def truncate(name: String): String = {
    val maxLength = 256
    if (name.length <= maxLength) name else name.substring(0, maxLength - 3) + "..."
  }

  /**
   * Distribute a local Scala Iterable to form an SCollection.
   * @group in_memory
   */
  def parallelize[T: ClassTag](elems: Iterable[T]): SCollection[T] = pipelineOp {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    wrap(this.applyInternal(Create.of(elems.asJava).withCoder(coder)))
      .setName(truncate(elems.toString()))
  }

  /**
   * Distribute a local Scala Map to form an SCollection.
   * @group in_memory
   */
  def parallelize[K: ClassTag, V: ClassTag](elems: Map[K, V]): SCollection[(K, V)] = pipelineOp {
    val coder = pipeline.getCoderRegistry.getScalaKvCoder[K, V]
    wrap(this.applyInternal(Create.of(elems.asJava).withCoder(coder)))
      .map(kv => (kv.getKey, kv.getValue))
      .setName(truncate(elems.toString()))
  }

  /**
   * Distribute a local Scala Iterable with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[(T, Instant)])
  : SCollection[T] = pipelineOp {
    val coder = pipeline.getCoderRegistry.getScalaCoder[T]
    val v = elems.map(t => TimestampedValue.of(t._1, t._2))
    wrap(this.applyInternal(Create.timestamped(v.asJava).withCoder(coder)))
      .setName(truncate(elems.toString()))
  }

  /**
   * Distribute a local Scala Iterable with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: ClassTag](elems: Iterable[T], timestamps: Iterable[Instant])
  : SCollection[T] = pipelineOp {
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
  def distCache[F](uri: String)(initFn: File => F): DistCache[F] = self.pipelineOp {
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
  def distCache[F](uris: Seq[String])(initFn: Seq[File] => F): DistCache[F] = self.pipelineOp {
    if (self.isTest) {
      new MockDistCache(testDistCache(DistCacheIO(uris.mkString("\t"))))
    } else {
      new DistCacheMulti(uris.map(new URI(_)), initFn, self.optionsAs[GcsOptions])
    }
  }

}
