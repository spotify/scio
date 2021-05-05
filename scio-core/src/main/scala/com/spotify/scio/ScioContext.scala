/*
 * Copyright 2019 Spotify AB.
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
import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io._
import com.spotify.scio.metrics.Metrics
import com.spotify.scio.options.ScioOptions
import com.spotify.scio.testing._
import com.spotify.scio.util._
import com.spotify.scio.values._
import org.apache.beam.runners.core.construction.resources.PipelineResources
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.metrics.Counter
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.{Pipeline, PipelineResult, io => beam}
import org.joda.time
import org.joda.time.Instant
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.collection.mutable.{Buffer => MBuffer}
import scala.concurrent.duration.Duration
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions

/** Runner specific context. */
trait RunnerContext {
  def prepareOptions(options: PipelineOptions, artifacts: List[String]): Unit
}

private case object NoOpContext extends RunnerContext {
  override def prepareOptions(options: PipelineOptions, artifacts: List[String]): Unit = ()
}

/** Direct runner specific context. */
private case object DirectContext extends RunnerContext {
  override def prepareOptions(options: PipelineOptions, artifacts: List[String]): Unit =
    // if in local runner, temp location may be needed, but is not currently required by
    // the runner, which may end up with NPE. If not set but user generate new temp dir
    if (options.getTempLocation == null) {
      val tmpDir = Files.createTempDirectory("scio-temp-")
      ScioContext.log.debug(s"New temp directory at $tmpDir")
      options.setTempLocation(tmpDir.toString)
    }
}

/** Companion object for [[RunnerContext]]. */
private object RunnerContext {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val mapping =
    Map(
      "DirectRunner" -> DirectContext.getClass.getName,
      "DataflowRunner" -> "com.spotify.scio.runners.dataflow.DataflowContext$",
      "SparkRunner" -> "com.spotify.scio.runners.spark.SparkContext$",
      "FlinkRunner" -> "com.spotify.scio.runners.flink.FlinkContext$"
    ).withDefaultValue(NoOpContext.getClass.getName)

  // FIXME: this is ugly, is there a better way?
  private def get(options: PipelineOptions): RunnerContext = {
    val runner = options.getRunner.getSimpleName
    val cls = mapping(runner)
    try {
      Class
        .forName(cls)
        .getField("MODULE$")
        .get(null)
        .asInstanceOf[RunnerContext]
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"Failed to load runner specific context $cls for $runner", e)
    }
  }

  def prepareOptions(options: PipelineOptions, artifacts: List[String]): Unit =
    get(options).prepareOptions(options, artifacts)

  // =======================================================================
  // Extra artifacts - jars/files etc
  // =======================================================================

  /** Compute list of local files to make available to workers. */
  def filesToStage(
    pipelineOptions: PipelineOptions,
    classLoader: ClassLoader,
    localArtifacts: Iterable[String],
    extraLocalArtifacts: Iterable[String]
  ): Iterable[String] = {
    val artifacts = if (localArtifacts.isEmpty) {
      detectClassPathResourcesToStage(pipelineOptions, classLoader)
    } else {
      localArtifacts
    }

    val finalLocalArtifacts = artifacts ++ extraLocalArtifacts.iterator
      .map {
        case path if path.endsWith(".jar") =>
          path.substring(path.lastIndexOf("/") + 1, path.length) -> path
        case path =>
          path -> path
      }
      .toMap
      .values

    logger.debug(s"Final list of extra artifacts: ${finalLocalArtifacts.mkString(":")}")
    finalLocalArtifacts
  }

  /** Borrowed from DataflowRunner. */
  private[this] def detectClassPathResourcesToStage(
    pipelineOptions: PipelineOptions,
    classLoader: ClassLoader
  ): Iterable[String] = {
    val sanitize: String => String = _.replace("\\", "/")
    val matchesEnvDir: String => Boolean =
      _.matches(s"${sanitize(sys.props("user.home"))}/\\..+/.+")
    val classPathJars = PipelineResources
      .detectClassPathResourcesToStage(classLoader, pipelineOptions)
      .asScala
      .filterNot(sanitize.andThen(matchesEnvDir))

    logger.debug(s"Classpath jars: ${classPathJars.mkString(":")}")

    classPathJars
  }

}

/** Convenience object for creating [[ScioContext]] and [[Args]]. */
object ContextAndArgs {
  trait ArgsParser[F[_]] {
    type ArgsType
    type UsageOrHelp = String
    type Result = Either[UsageOrHelp, (PipelineOptions, ArgsType)]

    def parse(args: Array[String]): F[Result]
  }

  final case class DefaultParser[T <: PipelineOptions: ClassTag] private[scio] ()
      extends ArgsParser[Try] {
    override type ArgsType = Args

    override def parse(args: Array[String]): Try[Result] = Try {
      Right(ScioContext.parseArguments[T](args))
    }
  }

  import caseapp._
  import caseapp.core.help._
  import caseapp.core.parser.ParserWithNameFormatter

  object TypedParser {
    final def apply[T]()(implicit parser: Parser[T], help: Help[T]): TypedParser[T] = {
      val parserOverride = parser match {
        case p: ParserWithNameFormatter[T] => p
        case p                             => p.nameFormatter(_.name)
      }
      val helpOverride =
        help.withNameFormatter(parserOverride.defaultNameFormatter).asInstanceOf[Help[T]]
      new TypedParser()(parserOverride, helpOverride)
    }
  }

  final class TypedParser[T: Parser: Help] private () extends ArgsParser[Try] {
    override type ArgsType = T

    override def parse(args: Array[String]): Try[Result] = {
      // limit the options passed to case-app
      // to options supported in T
      // fail if there are unsupported options
      val supportedCustomArgs =
        Parser[T].args
          .flatMap(a => a.name +: a.extraNames)
          .map(Parser[T].defaultNameFormatter.format) ++ List("help", "usage")

      val Reg = "^-{1,2}(.+)$".r
      val (customArgs, remainingArgs) =
        args.partition {
          case Reg(a) =>
            val name = a.takeWhile(_ != '=')
            supportedCustomArgs.contains(name)
          case _ => true
        }

      CaseApp.detailedParseWithHelp[T](customArgs.toIndexedSeq) match {
        case Left(error) =>
          Failure(new Exception(error.message))
        case Right((_, _, help, _)) if help =>
          Success(Left(Help[T].help))
        case Right((_, usage, _, _)) if usage =>
          val sysProps = SysProps.properties.map(_.show).mkString("\n")
          val baos = new ByteArrayOutputStream()
          val pos = new PrintStream(baos)

          for {
            i <- PipelineOptionsFactory.getRegisteredOptions.asScala
          } PipelineOptionsFactory.printHelp(pos, i)
          pos.close()

          val msg = sysProps + Help[T].help + new String(baos.toByteArray, StandardCharsets.UTF_8)
          Success(Left(msg))
        case Right((Right(t), _, _, _)) =>
          val (opts, unused) = ScioContext.parseArguments[PipelineOptions](remainingArgs)
          val unusedMap = unused.asMap
          if (unusedMap.isEmpty) {
            Success(Right((opts, t)))
          } else {
            val msg = "Unknown arguments: " + unusedMap.keys.mkString(", ")
            Failure(new Exception(msg))
          }
        case Right((Left(error), _, _, _)) =>
          Failure(new Exception(error.message))
      }
    }
  }

  final case class PipelineOptionsParser[T <: PipelineOptions: ClassTag] private[scio] ()
      extends ArgsParser[Try] {
    override type ArgsType = T

    override def parse(args: Array[String]): Try[Result] = Try {
      val (opts, remainingArgs) = ScioContext.parseArguments[T](args, withValidation = true)
      Either.cond(remainingArgs.asMap.isEmpty, (opts, opts), s"Unused $remainingArgs")
    } match {
      case r @ Success(Right(_)) => r
      case Success(Left(v))      => Failure(new Exception(v))
      case f                     => f
    }
  }

  sealed trait TypedArgsParser[T, F[_]] {
    def parser: ArgsParser[F]
  }

  sealed trait LowPrioTypedArgsParser {
    implicit def caseApp[T: Parser: Help]: TypedArgsParser[T, Try] = new TypedArgsParser[T, Try] {
      override def parser: ArgsParser[Try] = TypedParser[T]()
    }
  }

  object TypedArgsParser extends LowPrioTypedArgsParser {
    implicit def pipelineOptions[T <: PipelineOptions: ClassTag]: TypedArgsParser[T, Try] =
      new TypedArgsParser[T, Try] {
        override def parser: ArgsParser[Try] = PipelineOptionsParser[T]()
      }
  }

  def withParser[T](parser: ArgsParser[Try]): Array[String] => (ScioContext, T) =
    args =>
      parser.parse(args) match {
        case Failure(exception) =>
          throw exception
        case Success(Left(usageOrHelp)) =>
          Console.out.println(usageOrHelp)

          UsageOrHelpException.attachUncaughtExceptionHandler()
          throw new UsageOrHelpException()
        case Success(Right((_opts, _args))) =>
          (new ScioContext(_opts, Nil), _args.asInstanceOf[T])
      }

  /** Create [[ScioContext]] and [[Args]] for command line arguments. */
  def apply(args: Array[String]): (ScioContext, Args) =
    withParser(DefaultParser[PipelineOptions]()).apply(args)

  def typed[T](args: Array[String])(implicit tap: TypedArgsParser[T, Try]): (ScioContext, T) =
    withParser(tap.parser).apply(args)

  private[scio] class UsageOrHelpException extends Exception with NoStackTrace

  private[scio] object UsageOrHelpException {
    def attachUncaughtExceptionHandler(): Unit = {
      val currentThread = Thread.currentThread()
      val originalHandler = currentThread.getUncaughtExceptionHandler
      currentThread.setUncaughtExceptionHandler(
        new Thread.UncaughtExceptionHandler {
          def uncaughtException(thread: Thread, exception: Throwable): Unit =
            exception match {
              case _: UsageOrHelpException =>
                sys.exit(0)
              case _ =>
                originalHandler.uncaughtException(thread, exception)
            }
        }
      )
    }
  }
}

/**
 * ScioExecutionContext is the result of [[ScioContext#run()]].
 *
 * This is a handle to the underlying running job and allows getting the state,
 * checking if it's completed and to wait for it's execution.
 */
trait ScioExecutionContext {
  def pipelineResult: PipelineResult

  /** Whether the pipeline is completed. */
  def isCompleted: Boolean

  /** Pipeline's current state. */
  def state: State

  /** default await duration when using [[waitUntilFinish]] or [[waitUntilDone]] */
  def awaitDuration: Duration

  /** default cancel job option when using [[waitUntilFinish]] or [[waitUntilDone]] */
  def cancelJob: Boolean

  /**
   * Wait until the pipeline finishes. If timeout duration is exceeded and `cancelJob` is set,
   * cancel the internal [[PipelineResult]].
   */
  def waitUntilFinish(
    duration: Duration = awaitDuration,
    cancelJob: Boolean = cancelJob
  ): ScioResult

  /**
   * Wait until the pipeline finishes with the State `DONE` (as opposed to `CANCELLED` or
   * `FAILED`). Throw exception otherwise.
   */
  def waitUntilDone(
    duration: Duration = awaitDuration,
    cancelJob: Boolean = cancelJob
  ): ScioResult
}

/** Companion object for [[ScioContext]]. */
object ScioContext {
  private[scio] val log = LoggerFactory.getLogger(this.getClass)

  import org.apache.beam.sdk.options.PipelineOptionsFactory

  /** Create a new [[ScioContext]] instance. */
  def apply(): ScioContext = ScioContext(defaultOptions)

  /** Create a new [[ScioContext]] instance. */
  def apply(options: PipelineOptions): ScioContext =
    new ScioContext(options, Nil)

  /** Create a new [[ScioContext]] instance. */
  def apply(artifacts: List[String]): ScioContext =
    new ScioContext(defaultOptions, artifacts)

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
  @tailrec
  def parseArguments[T <: PipelineOptions: ClassTag](
    cmdlineArgs: Array[String],
    withValidation: Boolean = false
  ): (T, Args) = {
    val optClass = ScioUtil.classOf[T]
    PipelineOptionsFactory.register(optClass)

    // Extract --pattern of all registered derived types of PipelineOptions
    val registeredPatterns = for {
      cls <- PipelineOptionsFactory.getRegisteredOptions.asScala
      method <- cls.getMethods()
      name = method.getName
      str <-
        if (
          (!name.startsWith("get") && !name.startsWith("is")) ||
          method.getParameterTypes.nonEmpty || method.getReturnType == classOf[Unit]
        ) {
          None
        } else {
          Some(Introspector.decapitalize(name.substring(if (name.startsWith("is")) 2 else 3)))
        }
    } yield s"--$str($$|=)".r

    val patterns = registeredPatterns.union(Set("--help($$|=)".r))

    // Split cmdlineArgs into 2 parts, optArgs for PipelineOptions and appArgs for Args
    val (optArgs, appArgs) =
      cmdlineArgs.partition(arg => patterns.exists(_.findFirstIn(arg).isDefined))

    val pipelineOpts = if (withValidation) {
      PipelineOptionsFactory.fromArgs(optArgs: _*).withValidation().as(optClass)
    } else {
      PipelineOptionsFactory.fromArgs(optArgs: _*).as(optClass)
    }

    val optionsFile = pipelineOpts.as(classOf[ScioOptions]).getOptionsFile
    if (optionsFile != null) {
      log.info(s"Appending options from $optionsFile")
      parseArguments(
        cmdlineArgs.filterNot(_.startsWith("--optionsFile=")) ++
          Source.fromFile(optionsFile).getLines()
      )
    } else {
      val args = Args(appArgs)
      if (appArgs.nonEmpty) {
        val argString = args.toString("", ", ", "")
        val sanitizedArgString =
          if (argString.length > appArgStringMaxLength) {
            log.warn("Truncating long app arguments")
            argString.substring(0, appArgStringMaxLength) + " [...]"
          } else {
            argString
          }

        pipelineOpts
          .as(classOf[ScioOptions])
          .setAppArguments(sanitizedArgString)
      }
      (pipelineOpts, args)
    }
  }

  // Used to trim app args for UI if too long to avoid
  // contributing to an exceeded upload size limit.
  private val appArgStringMaxLength = 50000

  /** Implicit conversion from ScioContext to DistCacheScioContext. */
  implicit def makeDistCacheScioContext(self: ScioContext): DistCacheScioContext =
    new DistCacheScioContext(self)

  private def defaultOptions: PipelineOptions = PipelineOptionsFactory.create()

  private[scio] def validateOptions(o: PipelineOptions): Unit = {
    VersionUtil.checkVersion()
    VersionUtil.checkRunnerVersion(o.getRunner)

    // Check if running within scala.App. See https://github.com/spotify/scio/issues/449
    if (
      Thread
        .currentThread()
        .getStackTrace
        .toList
        .map(_.getClassName.split('$').head)
        .exists(_.equals(classOf[App].getName))
    ) {
      ScioContext.log.warn(
        "Applications defined within scala.App might not work properly. Please use main method!"
      )
    }
  }
}

/**
 * Main entry point for Scio functionality. A ScioContext represents a pipeline and can be used to
 * create SCollections and distributed caches on that cluster.
 *
 * @groupname dist_cache Distributed Cache
 * @groupname in_memory In-memory Collections
 * @groupname input Input Sources
 * @groupname Ungrouped Other Members
 */
class ScioContext private[scio] (
  val options: PipelineOptions,
  private var artifacts: List[String]
) extends TransformNameable {
  // var _pipeline member is lazily initialized, this makes sure that file systems are registered
  // before any IO
  FileSystems.setDefaultPipelineOptions(options)

  /** Get PipelineOptions as a more specific sub-type. */
  def optionsAs[T <: PipelineOptions: ClassTag]: T =
    options.as(ScioUtil.classOf[T])

  // Set default name if no app name specified by user
  Try(optionsAs[ApplicationNameOptions]).foreach { o =>
    if (o.getAppName == null || o.getAppName.startsWith("ScioContext$")) {
      this.setAppName(CallSites.getAppName)
    }
  }

  // Set default job name if none specified by user
  if (options.getJobName == null) {
    options.setJobName(optionsAs[ApplicationNameOptions].getAppName) // appName already set
  }

  {
    val o = optionsAs[ScioOptions]
    o.setScalaVersion(BuildInfo.scalaVersion)
    o.setScioVersion(BuildInfo.version)
  }

  private[scio] def labels: Map[String, String] =
    (for {
      // Check if class is in classpath
      _ <- Try(Class.forName("org.apache.beam.runners.dataflow.options.DataflowPipelineOptions"))
      o <- Try(optionsAs[DataflowPipelineOptions])
    } yield o.getLabels.asScala.toMap).getOrElse(Map.empty)

  private[scio] val testId: Option[String] =
    Try(optionsAs[ApplicationNameOptions]).toOption.map(_.getAppName).filter(TestUtil.isTestId)

  /** Amount of time to block job for. */
  private[scio] val awaitDuration: Duration = {
    val blockFor = optionsAs[ScioOptions].getBlockFor
    try {
      Option(blockFor)
        .map(Duration(_))
        .getOrElse(Duration.Inf)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"blockFor param $blockFor cannot be cast to " +
            s"type scala.concurrent.duration.Duration"
        )
    }
  }

  if (isTest) {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create)
  }

  private[scio] def prepare(): Unit = {
    // TODO: make sure this works for other PipelineOptions
    RunnerContext.prepareOptions(options, artifacts)
    ScioContext.validateOptions(options)
  }

  /** Underlying pipeline. */
  def pipeline: Pipeline = {
    if (_pipeline == null) {
      prepare()
      _pipeline = if (testId.isEmpty) {
        Pipeline.create(options)
      } else {
        TestDataManager.startTest(testId.get)
        // load TestPipeline dynamically to avoid ClassNotFoundException when running src/main
        // https://issues.apache.org/jira/browse/BEAM-298
        val cls = Class.forName("org.apache.beam.sdk.testing.TestPipeline")
        val tp = cls
          .getMethod("fromOptions", classOf[PipelineOptions])
          .invoke(null, options)
          .asInstanceOf[Pipeline]
        // workaround for @Rule enforcement introduced by
        // https://issues.apache.org/jira/browse/BEAM-1205
        cls
          .getMethod("enableAbandonedNodeEnforcement", classOf[Boolean])
          .invoke(tp, Boolean.box(true))
        tp
      }
    }

    _pipeline
  }

  /* Mutable members */
  private var _pipeline: Pipeline = _
  private var _isClosed: Boolean = false
  private val _counters: MBuffer[Counter] = MBuffer.empty
  private var _onClose: Unit => Unit = identity

  /** Wrap a [[org.apache.beam.sdk.values.PCollection PCollection]]. */
  def wrap[T](p: PCollection[T]): SCollection[T] =
    new SCollectionImpl[T](p, this)

  /** Add callbacks calls when the context is closed. */
  private[scio] def onClose(f: Unit => Unit): Unit =
    _onClose = _onClose compose f

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

  /** Set job name for the context. */
  def setJobName(name: String): Unit = {
    if (_pipeline != null) {
      throw new RuntimeException("Cannot set job name once pipeline is initialized")
    }
    options.setJobName(name)
  }

  /**
   * Runs the underlying pipeline.
   *
   * Running closes the context and no further transformations can be applied to the
   * pipeline once the context is closed.
   *
   * @return the [[ScioExecutionContext]] for the underlying job execution.
   */
  def run(): ScioExecutionContext = requireNotClosed {
    _onClose(())

    if (_counters.nonEmpty) {
      val counters = _counters.toArray
      this.parallelize(Seq(0)).withName("Initialize counters").map { _ =>
        counters.foreach(_.inc(0))
      }
    }

    _isClosed = true

    val context = execute()

    testId.foreach { id =>
      val result = context.waitUntilDone()
      TestDataManager.closeTest(id, result)
    }

    context
  }

  private[scio] def execute(): ScioExecutionContext = {
    val sc = this
    val pr = pipeline.run()

    new ScioExecutionContext {
      override val pipelineResult: PipelineResult = pr

      override def isCompleted: Boolean = state.isTerminal()

      override def state: State = Try(pipelineResult.getState).getOrElse(State.UNKNOWN)

      override val cancelJob: Boolean = true

      override val awaitDuration: Duration = sc.awaitDuration

      override def waitUntilFinish(duration: Duration, cancelJob: Boolean): ScioResult = {
        val wait = duration match {
          // according to PipelineResult values <= 1 ms mean `Duration.Inf`
          case Duration.Inf => -1
          case d            => d.toMillis
        }
        // according to PipelineResult returns null on timeout
        val state = pipelineResult.waitUntilFinish(time.Duration.millis(wait))
        if (state == null && cancelJob) {
          pipelineResult.cancel()
          val finalState = pipelineResult.getState()
          val cause = new InterruptedException(
            s"Job cancelled after exceeding timeout value $duration with state $finalState"
          )
          throw new PipelineExecutionException(cause)
        }

        new ScioResult(pipelineResult) {
          private val metricsLocation = sc.optionsAs[ScioOptions].getMetricsLocation
          if (metricsLocation != null) {
            saveMetrics(metricsLocation)
          }

          override def getMetrics: Metrics =
            Metrics(
              BuildInfo.version,
              BuildInfo.scalaVersion,
              sc.optionsAs[ApplicationNameOptions].getAppName,
              this.state.toString,
              getBeamMetrics
            )

          override def isTest: Boolean = sc.isTest
        }
      }

      override def waitUntilDone(duration: Duration, cancelJob: Boolean): ScioResult = {
        val result = waitUntilFinish(duration, cancelJob)
        if (!state.equals(State.DONE)) {
          throw new PipelineExecutionException(new Exception(s"Job finished with state $state"))
        }

        result
      }
    }
  }

  /** Whether the context is closed. */
  def isClosed: Boolean = _isClosed

  /** Ensure an operation is called before the pipeline has already been executed. */
  private[scio] def requireNotClosed[T](body: => T): T = {
    require(!this.isClosed, "Pipeline cannot be modified once ScioContext has been executed")
    body
  }

  // =======================================================================
  // Test wiring
  // =======================================================================

  /** Whether this is a test context. */
  def isTest: Boolean = testId.isDefined

  // =======================================================================
  // Read operations
  // =======================================================================

  private[scio] def applyInternal[Output <: POutput](
    name: Option[String],
    root: PTransform[_ >: PBegin, Output]
  ): Output =
    pipeline.apply(this.tfName(name), root)

  private[scio] def applyInternal[Output <: POutput](
    root: PTransform[_ >: PBegin, Output]
  ): Output =
    applyInternal(None, root)

  private[scio] def applyInternal[Output <: POutput](
    name: String,
    root: PTransform[_ >: PBegin, Output]
  ): Output =
    applyInternal(Option(name), root)

  private[scio] def applyTransform[U](
    name: Option[String],
    root: PTransform[_ >: PBegin, PCollection[U]]
  ): SCollection[U] =
    wrap(applyInternal(name, root))

  private[scio] def applyTransform[U](
    root: PTransform[_ >: PBegin, PCollection[U]]
  ): SCollection[U] =
    applyTransform(None, root)

  private[scio] def applyTransform[U](
    name: String,
    root: PTransform[_ >: PBegin, PCollection[U]]
  ): SCollection[U] =
    applyTransform(Option(name), root)

  /**
   * Get an SCollection for a text file.
   * @group input
   */
  def textFile(
    path: String,
    compression: beam.Compression = beam.Compression.AUTO
  ): SCollection[String] =
    this.read(TextIO(path))(TextIO.ReadParam(compression))

  /**
   * Get an SCollection with a custom input transform. The transform should have a unique name.
   * @group input
   */
  def customInput[T, I >: PBegin <: PInput](
    name: String,
    transform: PTransform[I, PCollection[T]]
  ): SCollection[T] =
    requireNotClosed {
      if (this.isTest) {
        TestDataManager.getInput(testId.get)(CustomIO[T](name)).toSCollection(this)
      } else {
        applyTransform(name, transform)
      }
    }

  /**
   * Generic read method for all `ScioIO[T]` implementations, which will invoke the provided IO's
   * [[com.spotify.scio.io.ScioIO[T]#readWithContext]] method along with read configurations
   * passed in. The IO class can delegate test-specific behavior if necessary.
   *
   * @param io     an implementation of `ScioIO[T]` trait
   * @param params configurations need to pass to perform underline read implementation
   */
  def read[T](io: ScioIO[T])(params: io.ReadP): SCollection[T] =
    io.readWithContext(this, params)

  def read[T](io: ScioIO[T] { type ReadP = Unit }): SCollection[T] =
    io.readWithContext(this, ())

  // =======================================================================
  // In-memory collections
  // =======================================================================

  /** Create a union of multiple SCollections. Supports empty lists. */
  // `T: Coder` context bound is required since `scs` might be empty.
  def unionAll[T: Coder](scs: Iterable[SCollection[T]]): SCollection[T] =
    scs match {
      case Nil => empty()
      case contents =>
        wrap(
          PCollectionList
            .of(contents.map(_.internal).asJava)
            .apply(this.tfName, Flatten.pCollections())
        )
    }

  /** Form an empty SCollection. */
  def empty[T: Coder](): SCollection[T] = parallelize(Nil)

  /**
   * Distribute a local Scala `Iterable` to form an SCollection.
   * @group in_memory
   */
  def parallelize[T: Coder](elems: Iterable[T]): SCollection[T] =
    requireNotClosed {
      val coder = CoderMaterializer.beam(this, Coder[T])
      this.applyTransform(Create.of(elems.asJava).withCoder(coder))
    }

  /**
   * Distribute a local Scala `Map` to form an SCollection.
   * @group in_memory
   */
  def parallelize[K, V](
    elems: Map[K, V]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, V)] =
    requireNotClosed {
      val kvc = CoderMaterializer.kvCoder[K, V](this)
      this
        .applyTransform(Create.of(elems.asJava).withCoder(kvc))
        .map(kv => (kv.getKey, kv.getValue))
    }

  /**
   * Distribute a local Scala `Iterable` with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: Coder](elems: Iterable[(T, Instant)]): SCollection[T] =
    requireNotClosed {
      val coder = CoderMaterializer.beam(this, Coder[T])
      val v = elems.map(t => TimestampedValue.of(t._1, t._2))
      this.applyTransform(Create.timestamped(v.asJava).withCoder(coder))
    }

  /**
   * Distribute a local Scala `Iterable` with timestamps to form an SCollection.
   * @group in_memory
   */
  def parallelizeTimestamped[T: Coder](
    elems: Iterable[T],
    timestamps: Iterable[Instant]
  ): SCollection[T] =
    requireNotClosed {
      val coder = CoderMaterializer.beam(this, Coder[T])
      val v = elems.zip(timestamps).map(t => TimestampedValue.of(t._1, t._2))
      this.applyTransform(Create.timestamped(v.asJava).withCoder(coder))
    }

  // =======================================================================
  // Metrics
  // =======================================================================

  /**
   * Initialize a new [[org.apache.beam.sdk.metrics.Counter Counter]] metric using `T` as namespace.
   * Default is "com.spotify.scio.ScioMetrics" if `T` is not specified.
   */
  def initCounter[T: ClassTag](name: String): Counter =
    initCounter(ScioMetrics.counter[T](name)).head

  /**
   * Initialize a new [[org.apache.beam.sdk.metrics.Counter Counter]] metric from namespace and
   * name.
   */
  def initCounter(namespace: String, name: String): Counter =
    initCounter(ScioMetrics.counter(namespace, name)).head

  /** Initialize a given [[org.apache.beam.sdk.metrics.Counter Counter]] metric. */
  def initCounter(counters: Counter*): Seq[Counter] = {
    _counters.appendAll(counters)
    counters
  }
}

/** An enhanced ScioContext with distributed cache features. */
class DistCacheScioContext private[scio] (self: ScioContext) {
  private[scio] def testDistCache: TestDistCache =
    TestDataManager.getDistCache(self.testId.get)

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
    self.requireNotClosed {
      if (self.isTest) {
        new MockDistCacheFunc(testDistCache(DistCacheIO(uri)))
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
        new MockDistCacheFunc(testDistCache(DistCacheIO(uris)))
      } else {
        new DistCacheMulti(uris.map(new URI(_)), initFn, self.optionsAs[GcsOptions])
      }
    }
}
