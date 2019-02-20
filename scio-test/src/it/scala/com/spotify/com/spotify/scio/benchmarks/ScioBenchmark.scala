/*
 * Copyright 2018 Spotify AB.
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

import com.spotify.scio._
import com.spotify.scio.coders.Coder
import com.spotify.scio.runners.dataflow.DataflowResult
import com.spotify.scio.values.SCollection
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataflow.model.{Job, JobMetrics}
import com.google.api.services.dataflow.{Dataflow => GDataflow, DataflowScopes}
import com.google.common.reflect.ClassPath
import com.google.datastore.v1._
import com.google.datastore.v1.client.{Datastore, DatastoreHelper}
import org.apache.beam.sdk.PipelineResult.State
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat, PeriodFormat}
import org.joda.time.{DateTimeZone, Instant, LocalDateTime, Seconds}
import shapeless.datatype.datastore._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.higherKinds
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
    Array("--runner=DataflowRunner",
          s"--numWorkers=$NumOfWorkers",
          s"--workerMachineType=$machineType",
          "--autoscalingAlgorithm=NONE")

  val ShuffleConf = Map("ShuffleService" -> Array("--experiments=shuffle_mode=service"))

  val CircleCI: Option[CircleCIEnv] = {
    val isCircleCIRun = sys.env.get("CIRCLECI").contains("true")

    if (isCircleCIRun) {
      (sys.env.get("CIRCLE_BUILD_NUM"), sys.env.get("CIRCLE_SHA1")) match {
        case (Some(buildNumber), Some(gitHash)) =>
          Some(CircleCIEnv(buildNumber.toLong, gitHash))
        case _ =>
          throw new IllegalStateException(
            "CIRCLECI env variable is set but not " +
              "CIRCLE_BUILD_NUM and CIRCLE_SHA1")
      }
    } else {
      PrettyPrint.print("CircleCI",
                        "CIRCLECI env variable not found. Will not publish " +
                          "benchmark results to Datastore.")
      None
    }
  }

  val BatchMetrics = Set("Elapsed",
                         "TotalMemoryUsage",
                         "TotalPdUsage",
                         "TotalShuffleDataProcessed",
                         "TotalSsdUsage",
                         "TotalStreamingDataProcessed",
                         "TotalVcpuTime")

  val StreamingMetrics = Set(
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

  def benchmarks(regex: String): mutable.Set[Benchmark] = {
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
  }

  def logger: ScioBenchmarkLogger[Try] = ScioBenchmarkLogger[Try](
    ConsoleLogger(),
    new DatastoreLogger(BatchMetrics)
  )
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

trait BenchmarkLogger[F[_]] {
  def log(benchmarks: Iterable[BenchmarkResult]): F[Unit]
}

final case class ScioBenchmarkLogger[F[_]](loggers: BenchmarkLogger[F]*) {
  def log(benchmarks: BenchmarkResult*): Seq[F[Unit]] =
    loggers.map(_.log(benchmarks))
}

case class BenchmarkResult(
  name: String,
  elapsed: Option[Seconds],
  buildNum: Long,
  startTime: LocalDateTime,
  finishTime: Option[LocalDateTime],
  state: State,
  extraArgs: Array[String],
  metrics: Map[String, String],
  scioVersion: String,
  beamVersion: String
)

object BenchmarkResult {
  import ScioBenchmarkSettings._

  private val DateTimeParser = ISODateTimeFormat.dateTimeParser()

  def batch(name: String, extraArgs: Array[String], scioResult: ScioResult): BenchmarkResult = {
    require(scioResult.isCompleted)

    val job: Job = scioResult.as[DataflowResult].getJob
    val startTime: LocalDateTime = DateTimeParser.parseLocalDateTime(job.getCreateTime)
    val finishTime: LocalDateTime = DateTimeParser.parseLocalDateTime(job.getCurrentStateTime)
    val elapsedTime: Seconds = Seconds.secondsBetween(startTime, finishTime)

    val metrics: Map[String, String] = scioResult
      .as[DataflowResult]
      .getJobMetrics
      .getMetrics
      .asScala
      .filter(metric => BatchMetrics.contains(metric.getName.getName))
      .map(m => (m.getName.getName, m.getScalar.toString))
      .sortBy(_._1)
      .toMap

    BenchmarkResult(
      name,
      Some(elapsedTime),
      CircleCI.map(_.buildNum).getOrElse(-1L),
      startTime,
      Some(finishTime),
      scioResult.state,
      extraArgs,
      metrics,
      BuildInfo.version,
      BuildInfo.beamVersion
    )
  }

  def streaming(name: String,
                buildNum: Long,
                createTime: String,
                jobMetrics: JobMetrics): BenchmarkResult = {
    val startTime: LocalDateTime = DateTimeParser.parseLocalDateTime(createTime)

    val metrics = jobMetrics.getMetrics.asScala
      .filter(metric => StreamingMetrics.contains(metric.getName.getName))
      .map(m => (m.getName.getName, m.getScalar.toString))
      .sortBy(_._1)
      .toMap

    BenchmarkResult(name,
                    None,
                    buildNum,
                    startTime,
                    None,
                    State.RUNNING,
                    Array(),
                    metrics,
                    BuildInfo.version,
                    BuildInfo.beamVersion)
  }
}

object DatastoreLogger {

  final case class ScioBenchmarkRun(timestamp: Instant,
                                    gitHash: String,
                                    buildNum: Long,
                                    operation: String)

  lazy val Storage: Datastore = DatastoreHelper.getDatastoreFromEnv
  val Kind = "Benchmarks"
  val OrderByBuildNumQuery = s"SELECT * from ${Kind}_%s ORDER BY buildNum DESC LIMIT 2"
  val WhereBuildNumQuery = s"SELECT * from ${Kind}_%s WHERE buildNum = %s"
}

class DatastoreLogger(metricsToCompare: Set[String]) extends BenchmarkLogger[Try] {

  import DatastoreLogger._
  import ScioBenchmarkSettings.CircleCI

  def dsKeyId(benchmark: BenchmarkResult): String = benchmark.buildNum.toString

  // Save metrics to integration testing Datastore instance. Can't make this into a
  // transaction because DS limit is 25 entities per transaction.
  def log(benchmarks: Iterable[BenchmarkResult]): Try[Unit] = {
    CircleCI
      .map { env =>
        val now = new Instant()
        val dt = DatastoreType[ScioBenchmarkRun]

        val commits = benchmarks.map { benchmark =>
          val entity = dt
            .toEntityBuilder(ScioBenchmarkRun(now, env.gitHash, benchmark.buildNum, benchmark.name))
            .setKey(
              DatastoreHelper
                .makeKey(s"${Kind}_${benchmark.name}", dsKeyId(benchmark))
                .build())

          val metrics = benchmark.elapsed match {
            case Some(period) => Map("Elapsed" -> period.getSeconds.toString) ++ benchmark.metrics
            case _            => benchmark.metrics
          }

          metrics.foreach {
            case (key, value) =>
              val entityValue = DatastoreHelper.makeValue(value).build()
              entity.putProperties(key, entityValue)
          }

          val scioVersionValue = DatastoreHelper.makeValue(benchmark.scioVersion).build()
          entity.putProperties("ScioVersion", scioVersionValue)

          val beamVersionValue = DatastoreHelper.makeValue(benchmark.beamVersion).build()
          entity.putProperties("BeamVersion", beamVersionValue)

          Try {
            val commit = Storage.commit(
              CommitRequest
                .newBuilder()
                .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
                // Upsert means we can re-run a job for same build if necessary;
                // insert would trigger a Datastore exception
                .addMutations(Mutation.newBuilder().setUpsert(entity.build()).build())
                .build())

            (benchmark, commit)
          }
        }

        commits
          .foldLeft(Try(List[(BenchmarkResult, CommitResponse)]())) {
            case (Success(list), Success(value)) => Success(value :: list)
            case (Success(_), Failure(ex))       => Failure(ex)
            case (f @ Failure(_), _)             => f
          }
          .map(_.map(_._1.name))
          .map(metrics => printMetricsComparison(benchmarks.map(_.name)))
      }
      .getOrElse {
        Success(Unit)
      }
  }

  private def getMetrics(benchmarkName: String, buildNums: Option[(String, String)]) =
    buildNums match {
      case Some((prev, curr)) =>
        Seq(prev, curr)
          .map { b =>
            Storage.runQuery(
              RunQueryRequest
                .newBuilder()
                .setGqlQuery(
                  GqlQuery
                    .newBuilder()
                    .setAllowLiterals(true)
                    .setQueryString(WhereBuildNumQuery.format(benchmarkName, b))
                    .build()
                )
                .build())
          }
          .map(_.getBatch.getEntityResults(0).getEntity)
      case None =>
        val comparisonMetrics = Storage.runQuery(
          RunQueryRequest
            .newBuilder()
            .setGqlQuery(
              GqlQuery
                .newBuilder()
                .setAllowLiterals(true)
                .setQueryString(OrderByBuildNumQuery.format(benchmarkName))
                .build()
            )
            .build())

        comparisonMetrics.getBatch.getEntityResultsList.asScala
          .sortBy(_.getEntity.getKey.getPath(0).getName)
          .map(_.getEntity)
    }

  // TODO: move this to email generator
  def printMetricsComparison(benchmarks: Iterable[String],
                             buildNums: Option[(String, String)] = None): Unit = {
    benchmarks.foreach { benchmarkName =>
      try {
        val metrics = getMetrics(benchmarkName, buildNums)
        if (metrics.size == 2) {
          val opName = metrics.head.getKey.getPath(0).getKind.substring(Kind.length + 1)
          val props = metrics.map(_.getPropertiesMap.asScala)
          PrettyPrint.printSeparator()
          PrettyPrint.print("Benchmark", opName)

          val List(b1, b2) = metrics.map(_.getKey.getPath(0).getName).toList
          PrettyPrint.print("BuildNum", "%15s%15s%15s".format(b1, b2, "Delta"))

          metricsToCompare.foreach { k: String =>
            val List(prev, curr) = props.map(_(k).getStringValue.toDouble).toList
            val delta = (curr - prev) / prev * 100.0
            val signed = if (delta.isNaN) {
              "0.00%"
            } else {
              (if (delta > 0) "+" else "") + "%.2f%%".format(delta)
            }
            PrettyPrint.print(k, "%15.2f%15.2f%15s".format(prev, curr, signed))
          }
        }
      } catch {
        case e: Exception =>
          PrettyPrint
            .print(benchmarkName, s"Caught error fetching benchmark metrics from Datastore: $e")
      }
    }
  }
}

final case class ConsoleLogger() extends BenchmarkLogger[Try] {
  override def log(benchmarks: Iterable[BenchmarkResult]): Try[Unit] = Try {
    benchmarks.foreach { benchmark =>
      PrettyPrint.printSeparator()
      PrettyPrint.print("Benchmark", benchmark.name)
      PrettyPrint.print("Scio version", benchmark.scioVersion)
      PrettyPrint.print("Beam version", benchmark.beamVersion)
      PrettyPrint.print("Extra arguments", benchmark.extraArgs.mkString(" "))
      PrettyPrint.print("State", benchmark.state.toString)
      PrettyPrint.print("Create time", benchmark.startTime.toString())
      PrettyPrint.print("Finish time", benchmark.finishTime.map(_.toString()).getOrElse("N/A"))
      PrettyPrint.print(
        "Elapsed",
        benchmark.elapsed.map(period => PeriodFormat.getDefault.print(period)).getOrElse("N/A"))
      benchmark.metrics.foreach { kv =>
        PrettyPrint.print(kv._1, kv._2)
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
// sbt scio-test/it:runMain com.spotify.ScioBatchBenchmarkResult $buildNum1 $buildNum2
// where $buildNum1 and $buildNum2 are build number of "bench" jobs in CircleCI
object ScioBatchBenchmarkResult {
  import ScioBenchmarkSettings._

  def main(args: Array[String]): Unit =
    new DatastoreLogger(BatchMetrics)
      .printMetricsComparison(ScioBatchBenchmark.BenchmarkNames, Some((args(0), args(1))))
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

  override def run(projectId: String,
                   prefix: String,
                   args: Array[String]): Iterable[Future[BenchmarkResult]] = {
    val username = CoreSysProps.User.value
    configurations
      .map {
        case (confName, extraArgs) =>
          val (sc, _) =
            ContextAndArgs(Array(s"--project=$projectId") ++ args ++ extraArgs)
          sc.setAppName(confName)
          sc.setJobName(s"$prefix-$confName-$username".toLowerCase())
          run(sc)
          val result = sc.close()
          result.finalState.map(_ => BenchmarkResult.batch(confName, extraArgs, result))
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
      .applyTransform(ParDo.of(new FillDoFn(() =>
        ("key" + Random.nextInt(numUniqueKeys), UUID.randomUUID().toString))))
      .mapValues(Elem(_))

  def withRandomKey[T: Coder](n: Int): SCollection[T] => SCollection[(Int, T)] =
    _.keyBy(_ => Random.nextInt(n))

  private def partitions(n: Long,
                         numPartitions: Int = 100,
                         numOfWorkers: Int = NumOfWorkers): Iterable[Iterable[Long]] = {
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

  def runParallel(args: Array[String],
                  benchmarkPrefix: String,
                  benchmarks: mutable.Set[Benchmark]): Unit = {
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

  def runSequentially(args: Array[String],
                      benchmarkPrefix: String,
                      benchmarks: mutable.Set[Benchmark],
                      pipelineArgs: Array[String]): Unit = {
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
