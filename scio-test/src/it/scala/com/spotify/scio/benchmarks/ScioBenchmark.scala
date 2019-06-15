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

package com.spotify.scio.benchmarks

import java.util.UUID

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataflow.model.{Job, JobMetrics}
import com.google.api.services.dataflow.{DataflowScopes, Dataflow => GDataflow}
import com.google.common.reflect.ClassPath
import com.google.datastore.v1._
import com.google.datastore.v1.client.{Datastore, DatastoreHelper}
import com.spotify.scio._
import com.spotify.scio.benchmarks.BenchmarkResult.{Batch, BenchmarkType, Metric}
import com.spotify.scio.coders.Coder
import com.spotify.scio.runners.dataflow.DataflowResult
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat, PeriodFormat}
import org.joda.time.{DateTimeZone, Instant, LocalDateTime, Seconds}
import org.slf4j.{Logger, LoggerFactory}
import shapeless.datatype.datastore._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Random, Success, Try}

/**
 * Shared functions to manage benchmark jobs and write metrics to DataStore
 *
 * This file is symlinked to scio-bench/src/main/scala/com/spotify/ScioBenchmark.scala so
 * that it can run with past Scio releases.
 */
object ScioBenchmarkSettings {
  val DefaultProjectId: String = "data-integration-test"
  val NumOfWorkers = 4

  def commonArgs(machineType: String = "n1-standard-4"): Array[String] =
    Array(
      "--runner=DataflowRunner",
      s"--numWorkers=$NumOfWorkers",
      s"--workerMachineType=$machineType",
      "--autoscalingAlgorithm=NONE"
    )

  val ShuffleConf = Map("ShuffleService" -> Array("--experiments=shuffle_mode=service"))

  val CircleCI: Option[CircleCIEnv] = {
    val isCircleCIRun = sys.env.get("CIRCLECI").contains("true")

    if (isCircleCIRun) {
      (sys.env.get("CIRCLE_BUILD_NUM"), sys.env.get("CIRCLE_SHA1")) match {
        case (Some(buildNumber), Some(gitHash)) =>
          Some(CircleCIEnv(buildNumber.toLong, gitHash.take(7)))
        case _ =>
          throw new IllegalStateException(
            "CIRCLECI env variable is set but not " +
              "CIRCLE_BUILD_NUM and CIRCLE_SHA1"
          )
      }
    } else {
      PrettyPrint.print(
        "CircleCI",
        "CIRCLECI env variable not found. Will not publish " +
          "benchmark results to Datastore."
      )
      None
    }
  }

  def benchmarks(regex: String): Seq[Benchmark] = {
    ClassPath
      .from(Thread.currentThread().getContextClassLoader)
      .getAllClasses
      .asScala
      .filter(_.getName.matches(regex))
      .flatMap { ci =>
        val cls = ci.load()
        if (classOf[Benchmark] isAssignableFrom cls) {
          Some(cls.newInstance().asInstanceOf[Benchmark])
        } else {
          None
        }
      }
      .toSeq
      .sortBy(_.name)
  }

  def logger[A <: BenchmarkType]: ScioBenchmarkLogger[Try, A] = {
    val loggers = if (CircleCI.isDefined) {
      Seq(ConsoleLogger[A](), new DatastoreLogger[A]())
    } else {
      Seq(ConsoleLogger[A]())
    }
    ScioBenchmarkLogger[Try, A](loggers: _*)
  }
}

final case class CircleCIEnv(buildNum: Long, gitHash: String)

object DataflowProvider {
  val Dataflow: GDataflow = {
    val transport = GoogleNetHttpTransport.newTrustedTransport()
    val jackson = JacksonFactory.getDefaultInstance
    val credential = GoogleCredential.getApplicationDefault
      .createScoped(DataflowScopes.all())
    new GDataflow.Builder(transport, jackson, credential).build()
  }
}

trait BenchmarkLogger[F[_], A <: BenchmarkType] {
  def log(benchmarks: Iterable[BenchmarkResult[A]]): F[Unit]
}

final case class ScioBenchmarkLogger[F[_], A <: BenchmarkType](loggers: BenchmarkLogger[F, A]*) {
  def log(benchmarks: BenchmarkResult[A]*): Seq[F[Unit]] =
    loggers.map(_.log(benchmarks))
}

object BenchmarkResult {
  import ScioBenchmarkSettings._

  val logger: Logger = LoggerFactory.getLogger(getClass)

  sealed trait BenchmarkType
  final case class Batch() extends BenchmarkType
  final case class Streaming() extends BenchmarkType

  final case class Metric(name: String, value: Long)

  private val DateTimeParser = ISODateTimeFormat.dateTimeParser()
  private val BatchMetrics = Set(
    "Elapsed",
    "TotalMemoryUsage",
    "TotalPdUsage",
    "TotalShuffleDataProcessed",
    "TotalSsdUsage",
    "TotalStreamingDataProcessed",
    "TotalVcpuTime"
  )
  private val StreamingMetrics = Set(
    "CurrentMemoryUsage",
    "CurrentPdUsage",
    "CurrentVcpuCount",
    "TotalMemoryUsage",
    "TotalPdUsage",
    "TotalShuffleDataProcessed",
    "TotalSsdUsage",
    "TotalStreamingDataProcessed",
    "TotalVcpuTime",
    "SystemLag"
  )

  def batch(
    timestamp: Instant,
    name: String,
    extraArgs: Array[String],
    scioResult: ScioResult
  ): BenchmarkResult[Batch] = {

    val job: Job = scioResult.as[DataflowResult].getJob
    val startTime: LocalDateTime = DateTimeParser.parseLocalDateTime(job.getCreateTime)
    val finishTime: LocalDateTime = DateTimeParser.parseLocalDateTime(job.getCurrentStateTime)
    val elapsedTime: Long = Seconds.secondsBetween(startTime, finishTime).getSeconds.toLong

    val metrics: List[Metric] = Metric("Elapsed", elapsedTime) :: scioResult
      .as[DataflowResult]
      .getJobMetrics
      .getMetrics
      .asScala
      .filter(metric => BatchMetrics.contains(metric.getName.getName))
      .map { m =>
        val scalar = try {
          m.getScalar.toString.toLong
        } catch {
          case e: NumberFormatException =>
            logger.error(s"Failed to get metric $m", e)
            0
        }
        Metric(m.getName.getName, scalar)
      }
      .toList

    BenchmarkResult[Batch](
      timestamp,
      name,
      Some(elapsedTime),
      CircleCI.map(_.buildNum).getOrElse(-1L),
      CircleCI.map(_.gitHash).getOrElse("none"),
      job.getCreateTime,
      Some(job.getCurrentStateTime),
      scioResult.state.toString,
      extraArgs,
      metrics,
      BuildInfo.version,
      BuildInfo.beamVersion
    )
  }

  def streaming(
    timestamp: Instant,
    name: String,
    buildNum: Long,
    gitHash: String,
    createTime: String,
    jobMetrics: JobMetrics
  ): BenchmarkResult[Streaming] = {
    val metrics = jobMetrics.getMetrics.asScala
      .filter(metric => StreamingMetrics.contains(metric.getName.getName))
      .map { m =>
        val scalar = try {
          m.getScalar.toString.toLong
        } catch {
          case e: NumberFormatException =>
            logger.error(s"Failed to get metric $m", e)
            0
        }
        Metric(m.getName.getName, scalar)
      }
      .toList

    BenchmarkResult[Streaming](
      timestamp,
      name,
      None,
      buildNum,
      gitHash,
      createTime,
      None,
      State.RUNNING.toString,
      Array(),
      metrics,
      BuildInfo.version,
      BuildInfo.beamVersion
    )
  }
}

case class BenchmarkResult[A <: BenchmarkType](
  timestamp: Instant,
  name: String,
  elapsed: Option[Long],
  buildNum: Long,
  gitHash: String,
  startTime: String,
  finishTime: Option[String],
  state: String,
  extraArgs: Array[String],
  metrics: List[Metric],
  scioVersion: String,
  beamVersion: String
) {

  def compareMetrics(other: BenchmarkResult[A]): List[(Option[Metric], Option[Metric], Double)] = {
    val otherMetrics = other.metrics.map(m => (m.name, m)).toMap
    val thisMetrics = metrics.map(m => (m.name, m)).toMap

    (thisMetrics.keySet ++ otherMetrics.keySet)
      .map { k =>
        (thisMetrics.get(k), otherMetrics.get(k))
      }
      .flatMap {
        case (optCurr @ Some(curr), optPrev @ Some(prev)) =>
          val delta = (curr.value - prev.value) / prev.value * 100.0
          Some((optPrev, optCurr, delta))
        case (None, optPrev @ Some(_)) =>
          Some((optPrev, None, 0d))
        case (optCurr @ Some(_), None) =>
          Some((None, optCurr, 0d))
        case (None, None) =>
          None
      }
      .toList
  }
}

object DatastoreLogger {
  lazy val Storage: Datastore = DatastoreHelper.getDatastoreFromEnv
  val Kind = "Benchmarks"

  private def latestBuildNumQuery(benchmarkName: String) =
    f"SELECT * from ${Kind}_$benchmarkName%s ORDER BY buildNum DESC LIMIT 1"
  private def buildNumQuery(benchmarkName: String, buildNum: Long) =
    f"SELECT * from ${Kind}_$benchmarkName%s WHERE buildNum = $buildNum%d"
}

class DatastoreLogger[A <: BenchmarkType] extends BenchmarkLogger[Try, A] {

  import DatastoreLogger._

  def dsKeyId(benchmark: BenchmarkResult[A]): String = benchmark.buildNum.toString

  // Save metrics to integration testing Datastore instance. Can't make this into a
  // transaction because DS limit is 25 entities per transaction.
  def log(benchmarks: Iterable[BenchmarkResult[A]]): Try[Unit] = {
    val dt = DatastoreType[BenchmarkResult[A]]

    val commits = benchmarks.map { benchmark =>
      Try {
        latestBenchmark(benchmark.name).foreach(printMetricsComparison(_, benchmark))

        val entity = dt
          .toEntityBuilder(benchmark)
          .setKey(
            DatastoreHelper
              .makeKey(s"${Kind}_${benchmark.name}", dsKeyId(benchmark))
              .build()
          )
          .build()

        val commit = Storage.commit(
          CommitRequest
            .newBuilder()
            .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
            // Upsert means we can re-run a job for same build if necessary;
            // insert would trigger a Datastore exception
            .addMutations(Mutation.newBuilder().setUpsert(entity).build())
            .build()
        )

        (entity, commit)
      }
    }

    commits
      .foldLeft(Try(List.empty[(Entity, CommitResponse)])) {
        case (Success(list), Success(value)) => Success(value :: list)
        case (Success(_), Failure(ex))       => Failure(ex)
        case (f @ Failure(_), _)             => f
      }
      .map(_ => ())
  }

  def latestBenchmark(benchmarkName: String): Option[BenchmarkResult[A]] =
    benchmarksByBuildNums(benchmarkName, Nil).headOption

  def benchmarksByBuildNums(
    benchmarkName: String,
    buildNums: List[Long]
  ): List[BenchmarkResult[A]] = {
    val dt = DatastoreType[BenchmarkResult[A]]
    val query: String => RunQueryRequest = q =>
      RunQueryRequest
        .newBuilder()
        .setGqlQuery(
          GqlQuery
            .newBuilder()
            .setAllowLiterals(true)
            .setQueryString(q)
            .build()
        )
        .build()
    val results = buildNums match {
      case Nil =>
        query(latestBuildNumQuery(benchmarkName)) :: Nil
      case bns =>
        bns.map(bn => query(buildNumQuery(benchmarkName, bn)))
    }

    results
      .map(Storage.runQuery)
      .flatMap(_.getBatch.getEntityResultsList.asScala)
      .sortBy(_.getEntity.getKey.getPath(0).getName)
      .map(_.getEntity)
      .flatMap(dt.fromEntity(_))
  }

  def printMetricsComparison(previous: BenchmarkResult[A], current: BenchmarkResult[A]): Unit = {
    PrettyPrint.printSeparator()
    PrettyPrint.print("Benchmark", current.name)
    PrettyPrint.print(
      "BuildNum",
      "%15s%15s%15s".format(previous.buildNum, current.buildNum, "Delta")
    )

    current.compareMetrics(previous).foreach {
      case (prev, curr, delta) =>
        val value: Option[Metric] => String = _.map(_.value.toString).getOrElse("-")
        val name = curr.map(_.name).getOrElse(prev.map(_.name).getOrElse("N/A"))

        PrettyPrint.print(name, "%15s%15s%15.2f%%".format(value(prev), value(curr), delta))
    }
  }

  // TODO: move this to email generator
  def printMetricsComparison(benchmarks: Iterable[String], buildNums: List[Long]): Unit =
    benchmarks.foreach { benchmarkName =>
      try {
        val bs = benchmarksByBuildNums(benchmarkName, buildNums)
        if (bs.nonEmpty) {
          bs.zip(bs.tail).foreach {
            case (a, b) =>
              printMetricsComparison(a, b)
          }
        }
      } catch {
        case e: Exception =>
          PrettyPrint
            .print(benchmarkName, s"Caught error fetching benchmark metrics from Datastore: $e")
      }
    }
}

final case class ConsoleLogger[A <: BenchmarkType]() extends BenchmarkLogger[Try, A] {
  override def log(benchmarks: Iterable[BenchmarkResult[A]]): Try[Unit] = Try {
    benchmarks.foreach { benchmark =>
      PrettyPrint.printSeparator()
      PrettyPrint.print("Benchmark", benchmark.name)
      PrettyPrint.print("Scio version", benchmark.scioVersion)
      PrettyPrint.print("Beam version", benchmark.beamVersion)
      PrettyPrint.print("Extra arguments", benchmark.extraArgs.mkString(" "))
      PrettyPrint.print("State", benchmark.state.toString)
      PrettyPrint.print("Create time", benchmark.startTime)
      PrettyPrint.print("Finish time", benchmark.finishTime.getOrElse("N/A"))
      PrettyPrint.print(
        "Elapsed",
        benchmark.elapsed
          .map(period => PeriodFormat.getDefault.print(Seconds.seconds(period.toInt)))
          .getOrElse("N/A")
      )
      benchmark.metrics.foreach { kv =>
        PrettyPrint.print(kv.name, kv.value.toString)
      }
    }
  }
}

private[this] object PrettyPrint {
  @inline def printSeparator(numChars: Int = 80): Unit =
    // scalastyle:off regex
    println("=" * numChars)
  // scalastyle:on regex

  @inline def print(k: String, v: String): Unit =
    // scalastyle:off regex
    println("%-30s: %s".format(k, v))

  // scalastyle:on regex
}

// Usage:
// export DATASTORE_PROJECT_ID=data-integration-test
// sbt scio-test/it:runMain com.spotify.ScioBatchBenchmarkResult $buildNum1 $buildNum2...
// where $buildNum1 and $buildNum2 are build number of "bench" jobs in CircleCI
object ScioBatchBenchmarkResult {

  def main(args: Array[String]): Unit =
    new DatastoreLogger()
      .printMetricsComparison(ScioBatchBenchmark.BenchmarkNames, args.map(_.toLong).toList)
}

trait ScioJob {
  def run(projectId: String, prefix: String, args: Array[String]): Any
  def run(sc: ScioContext): Unit
}

abstract class Benchmark(val extraConfs: Map[String, Array[String]] = Map.empty) extends ScioJob {
  val name: String = this.getClass.getSimpleName.replaceAll("\\$$", "")

  private val configurations: Map[String, Array[String]] = {
    val base = Map(name -> Array.empty[String])
    val extra = if (extraConfs == null) {
      Map.empty
    } else {
      extraConfs.map(kv => (s"$name${kv._1}", kv._2))
    }
    base ++ extra
  }

  override def run(
    projectId: String,
    prefix: String,
    args: Array[String]
  ): Iterable[Future[BenchmarkResult[Batch]]] = {
    val username = CoreSysProps.User.value
    configurations
      .map {
        case (confName, extraArgs) =>
          Future {
            val (sc, _) =
              ContextAndArgs(Array(s"--project=$projectId") ++ args ++ extraArgs)
            sc.setAppName(confName)
            sc.setJobName(s"$prefix-$confName-$username".toLowerCase())
            run(sc)
            val result = sc.close().waitUntilDone()
            BenchmarkResult.batch(Instant.now(), confName, extraArgs, result)
          }
      }
  }
}

object Benchmark {
  import ScioBenchmarkSettings._

  final case class Elem[T](elem: T)

  def randomUUIDs(sc: ScioContext, n: Long): SCollection[Elem[String]] =
    sc.parallelize(partitions(n)).transform("UUID-generator") {
      _.flatten[Long]
        .applyTransform(ParDo.of(new FillDoFn(() => UUID.randomUUID().toString)))
        .map(Elem(_))
    }

  def randomKVs(sc: ScioContext, n: Long, numUniqueKeys: Int): SCollection[(String, Elem[String])] =
    sc.parallelize(partitions(n))
      .flatten[Long]
      .applyTransform(
        ParDo.of(
          new FillDoFn(() => ("key" + Random.nextInt(numUniqueKeys), UUID.randomUUID().toString))
        )
      )
      .mapValues(Elem(_))

  def withRandomKey[T: Coder](n: Int): SCollection[T] => SCollection[(Int, T)] =
    _.keyBy(_ => Random.nextInt(n))

  private def partitions(
    n: Long,
    numPartitions: Int = 100,
    numOfWorkers: Int = NumOfWorkers
  ): Iterable[Iterable[Long]] = {
    val chunks = numPartitions * numOfWorkers

    def loop(n: Long): Seq[Long] = {
      n match {
        case 0                    => Nil
        case x if x < chunks      => Seq(x)
        case x if x % chunks == 0 => Seq.fill(chunks)(x / chunks)
        case x =>
          val r = x % chunks
          loop(r) ++ loop(x - r)
      }
    }

    loop(n).grouped(numOfWorkers).toIterable
  }

  private class FillDoFn[T](val f: () => T) extends DoFn[Long, T] {
    @ProcessElement
    def processElement(c: DoFn[Long, T]#ProcessContext): Unit = {
      var i = 0L
      val n = c.element()
      while (i < n) {
        c.output(f())
        i += 1
      }
    }
  }
}

object BenchmarkRunner {

  def runParallel(
    args: Array[String],
    benchmarkPrefix: String,
    benchmarks: Seq[Benchmark]
  ): Unit = {
    val argz = Args(args)
    val regex = argz.getOrElse("regex", ".*")
    val projectId = argz.getOrElse("project", ScioBenchmarkSettings.DefaultProjectId)
    val prefix = createPrefix(argz, benchmarkPrefix)
    val results = benchmarks
      .filter(_.name.matches(regex))
      .flatMap(_.run(projectId, prefix, ScioBenchmarkSettings.commonArgs()))
    val future = Future.sequence(results.map(_.map(ScioBenchmarkSettings.logger.log(_))))
    Await.result(future, Duration.Inf)
  }

  private def createPrefix(args: Args, benchmarkPrefix: String) = {
    val name = args("name")
    val timestamp = DateTimeFormat
      .forPattern("yyyyMMddHHmmss")
      .withZone(DateTimeZone.UTC)
      .print(System.currentTimeMillis())
    s"$benchmarkPrefix-$name-$timestamp"
  }

  def runSequentially(
    args: Array[String],
    benchmarkPrefix: String,
    benchmarks: Seq[Benchmark],
    pipelineArgs: Array[String]
  ): Unit = {
    val argz = Args(args)
    val regex = argz.getOrElse("regex", ".*")
    val projectId = argz.getOrElse("project", ScioBenchmarkSettings.DefaultProjectId)
    benchmarks
      .filter(_.name.matches(regex))
      .foreach(j => {
        val prefix = createPrefix(argz, benchmarkPrefix)
        val results = j.run(projectId, prefix, pipelineArgs)
        val future = Future.sequence(results.map(_.map(ScioBenchmarkSettings.logger.log(_))))
        Await.result(future, Duration.Inf)
      })
  }
}
