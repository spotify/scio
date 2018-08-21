/*
 * Copyright 2017 Spotify AB.
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

package com.spotify

import java.util.UUID

import com.google.api.services.dataflow.model.Job
import com.google.common.reflect.ClassPath
import com.google.datastore.v1._
import com.google.datastore.v1.client.{Datastore, DatastoreHelper}
import com.spotify.scio._
import com.spotify.scio.runners.dataflow.DataflowResult
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Aggregator
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat, PeriodFormat}
import org.joda.time.{DateTimeZone, Instant, LocalDateTime, Seconds}
import shapeless.datatype.datastore._

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.higherKinds
import scala.util.{Failure, Random, Success, Try}

// This file is symlinked to scio-bench/src/main/scala/com/spotify/ScioBenchmark.scala so that it
// can run with past Scio releases.

private[this] object PrettyPrint {
  @inline def printSeparator(numChars: Int = 80): Unit = {
    // scalastyle:off regex
    println("=" * numChars)
    // scalastyle:on regex
  }

  @inline def print(k: String, v: String): Unit = {
    // scalastyle:off regex
    println("%-30s: %s".format(k, v))
    // scalastyle:on regex
  }
}

final case class CircleCIEnv(buildNum: Long, gitHash: String)

object ScioBenchmarkSettings {
  val defaultProjectId: String = "data-integration-test"
  val numOfWorkers = 4
  val commonArgs = Array(
    "--runner=DataflowRunner",
    s"--numWorkers=$numOfWorkers",
    "--workerMachineType=n1-standard-4",
    "--autoscalingAlgorithm=NONE")

  val shuffleConf = Map("ShuffleService" -> Array("--experiments=shuffle_mode=service"))

  val circleCIEnv: Option[CircleCIEnv] = {
    val isCircleCIRun = sys.env.get("CIRCLECI").contains("true")

    if (isCircleCIRun) {
      (sys.env.get("CIRCLE_BUILD_NUM"), sys.env.get("CIRCLE_SHA1")) match {
        case (Some(buildNumber), Some(gitHash)) =>
          Some(CircleCIEnv(buildNumber.toLong, gitHash))
        case _ => throw new IllegalStateException("CIRCLECI env variable is set but not " +
          "CIRCLE_BUILD_NUM and CIRCLE_SHA1")
      }
    } else {
      PrettyPrint.print("CircleCI", "CIRCLECI env variable not found. Will not publish " +
        "benchmark results to Datastore.")
      None
    }
  }
}

abstract class Benchmark(val extraConfs: Map[String, Array[String]] = null) {
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

  def run(projectId: String, prefix: String, args: Array[String]): Iterable[BenchmarkResult] = {
    val username = sys.props("user.name")
    configurations
      .map { case (confName, extraArgs) =>
        val (sc, _) = ContextAndArgs(Array(s"--project=$projectId") ++ args ++ extraArgs)
        sc.setAppName(confName)
        sc.setJobName(s"$prefix-$confName-$username".toLowerCase())
        run(sc)
        val result = sc.close()
        BenchmarkResult(confName, extraArgs, result)
      }
  }

  def run(sc: ScioContext): Unit
}

object BenchmarkResult {
  private val dateTimeParser = ISODateTimeFormat.dateTimeParser()
}

case class BenchmarkResult(name: String,
                           extraArgs: Array[String],
                           scioResult: ScioResult) {
  import BenchmarkResult._

  lazy val job: Job = scioResult.as[DataflowResult].getJob
  lazy val startTime: LocalDateTime = dateTimeParser.parseLocalDateTime(job.getCreateTime)
  lazy val finishTime: LocalDateTime = dateTimeParser.parseLocalDateTime(job.getCurrentStateTime)
  lazy val elapsedTime: Seconds = Seconds.secondsBetween(startTime, finishTime)

  lazy val metrics: Map[String, String] = scioResult.as[DataflowResult]
    .getJobMetrics
    .getMetrics
    .asScala
    .filter { m =>
      m.getName.getName.startsWith("Total") && !m.getName.getContext.containsKey("tentative")
    }
    .map(m => (m.getName.getName, m.getScalar.toString))
    .sortBy(_._1)
    .toMap
}

trait BenchmarkLogger[F[_]] {
  def log(benchmarks: Iterable[BenchmarkResult]): F[Unit]
}

final case class ScioBenchmarkLogger[F[_]](loggers: BenchmarkLogger[F]*) {
  def log(benchmarks: Iterable[BenchmarkResult]): Seq[F[Unit]] =
    loggers.map(_.log(benchmarks))
}

object DatastoreLogger {
  final case class ScioBenchmarkRun(timestamp: Instant,
                                    gitHash: String,
                                    buildNum: Long,
                                    operation: String)

  private lazy val Storage: Datastore = DatastoreHelper.getDatastoreFromEnv

  private val Kind = "Benchmarks"

  private val OrderByBuildNumQuery =
    s"SELECT * from ${Kind}_%s ORDER BY buildNum DESC LIMIT 2"

  private val Metrics = Set("Elapsed", "TotalMemoryUsage", "TotalPdUsage",
    "TotalShuffleDataProcessed", "TotalSsdUsage", "TotalStreamingDataProcessed", "TotalVcpuTime")
}

final case class DatastoreLogger(circleCIEnv: Option[CircleCIEnv]) extends BenchmarkLogger[Try] {
  import DatastoreLogger._

  // Save metrics to integration testing Datastore instance. Can't make this into a
  // transaction because DS limit is 25 entities per transaction.
  override def log(benchmarks: Iterable[BenchmarkResult]): Try[Unit] = {
    circleCIEnv.map { env =>
      val now = new Instant()
      val dt = DatastoreType[ScioBenchmarkRun]

      val commits = benchmarks.map { benchmark =>
        val entity = dt
          .toEntityBuilder(
            ScioBenchmarkRun(now, env.gitHash, env.buildNum, benchmark.name))
          .setKey(DatastoreHelper.makeKey(
            s"${Kind}_${benchmark.name}", env.buildNum.toString))

        val metrics = ("Elapsed", benchmark.elapsedTime.getSeconds.toString) :: benchmark.metrics
          .filter(metric => Metrics.contains(metric._1)).toList

        metrics.foreach {
          case (key, value) =>
            val entityValue = DatastoreHelper.makeValue(value).build()
            entity.putProperties(key, entityValue)
        }

        Try {
          val commit = Storage.commit(CommitRequest.newBuilder()
            .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
            // Upsert means we can re-run a job for same build if necessary; insert would trigger
            // a Datastore exception
            .addMutations(Mutation.newBuilder().setUpsert(entity.build()).build())
            .build())

          (benchmark, commit)
        }
      }

      commits
        .foldLeft(Try(List[(BenchmarkResult, CommitResponse)]())) {
          case (Success(list), Success(value)) => Success(value :: list)
          case (Success(_), Failure(ex)) => Failure(ex)
          case (f @ Failure(_), _) => f
        }
        .map(_.map(_._1.name))
        .map(printMetricsComparison)
    }.getOrElse {
      Success(Unit)
    }
  }

  // TODO: move this to email generator
  private[this] def printMetricsComparison(benchmarkNames: Iterable[String]): Unit = {
    benchmarkNames.foreach { benchmarkName =>
      try {
        val comparisonMetrics = Storage.runQuery(
          RunQueryRequest.newBuilder().setGqlQuery(
            GqlQuery.newBuilder()
              .setAllowLiterals(true)
              .setQueryString(OrderByBuildNumQuery.format(benchmarkName))
              .build()
          ).build())

        val metrics = comparisonMetrics.getBatch.getEntityResultsList.asScala
          .sortBy(_.getEntity.getKey.getPath(0).getName.toInt)
          .map(_.getEntity)
        if (metrics.size == 2) {
          val opName = metrics.head.getKey.getPath(0).getKind.substring(Kind.length + 1)
          val props = metrics.map(_.getPropertiesMap.asScala)
          PrettyPrint.printSeparator()
          PrettyPrint.print("Benchmark", opName)

          val List(b1, b2) = props.map(_("buildNum").getIntegerValue).toList
          PrettyPrint.print("BuildNum", "%15d%15d%15s".format(b1, b2, "Delta"))

          Metrics.foreach { k =>
            val List(prev, curr) = props.map(_(k).getStringValue.toDouble).toList
            val delta = (curr - prev) / curr * 100.0
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
      PrettyPrint.print("Extra arguments", benchmark.extraArgs.mkString(" "))
      PrettyPrint.print("State", benchmark.scioResult.state.toString)
      PrettyPrint.print("Create time", benchmark.startTime.toString())
      PrettyPrint.print("Finish time", benchmark.finishTime.toString())
      PrettyPrint.print("Elapsed", PeriodFormat.getDefault.print(benchmark.elapsedTime))
      benchmark.metrics.foreach { kv =>
        PrettyPrint.print(kv._1, kv._2)
      }
    }
  }
}

// scalastyle:off number.of.types
// scalastyle:off number.of.methods
object ScioBenchmark {
  import ScioBenchmarkSettings._

  def main(args: Array[String]): Unit = {
    val argz = Args(args)
    val name = argz("name")
    val regex = argz.getOrElse("regex", ".*")
    val projectId = argz.getOrElse("project", ScioBenchmarkSettings.defaultProjectId)
    val timestamp = DateTimeFormat.forPattern("yyyyMMddHHmmss")
      .withZone(DateTimeZone.UTC)
      .print(System.currentTimeMillis())
    val prefix = s"ScioBenchmark-$name-$timestamp"
    val results = benchmarks
      .filter(_.name.matches(regex))
      .flatMap(_.run(projectId, prefix, commonArgs))

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future.sequence(results.map(_.scioResult.finalState))
    Await.result(future, Duration.Inf)

    ScioBenchmarkLogger[Try](ConsoleLogger(), DatastoreLogger(circleCIEnv)).log(results)
  }

  // =======================================================================
  // Benchmarks
  // =======================================================================

  private val benchmarks = ClassPath.from(Thread.currentThread().getContextClassLoader)
    .getAllClasses
    .asScala
    .filter(_.getName.matches("com\\.spotify\\.ScioBenchmark\\$[\\w]+\\$"))
    .flatMap { ci =>
      val cls = ci.load()
      if (classOf[Benchmark] isAssignableFrom cls) {
        Some(cls.newInstance().asInstanceOf[Benchmark])
      } else {
        None
      }
    }

  // ===== Combine =====

  // 100M items, into a set of 1000 unique items

  object Reduce extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).map(Set(_)).reduce(_ ++ _)
  }

  object Sum extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).map(Set(_)).sum
  }

  object Fold extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).map(Set(_)).fold(Set.empty[Int])(_ ++ _)
  }

  object FoldMonoid extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).map(Set(_)).fold
  }

  object Aggregate extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).aggregate(Set.empty[Int])(_ + _, _ ++ _)
  }

  object AggregateAggregator extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000)
        .aggregate(Aggregator.fromMonoid[Set[Int]].composePrepare[Int](Set(_)))
  }

  object Combine extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).combine(Set(_))(_ + _)(_ ++ _)
  }

  // ===== CombineByKey =====

  // 100M items, 10K keys, into a set of 1000 unique items per key

  object ReduceByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .mapValues(Set(_)).reduceByKey(_ ++ _)
  }

  object SumByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .mapValues(Set(_)).sumByKey
  }

  object FoldByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .mapValues(Set(_)).foldByKey(Set.empty[Int])(_ ++ _)
  }

  object FoldByKeyMonoid extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .mapValues(Set(_)).foldByKey
  }

  object AggregateByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .aggregateByKey(Set.empty[Int])(_ + _, _ ++ _)
  }

  object AggregateByKeyAggregator extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .aggregateByKey(Aggregator.fromMonoid[Set[Int]].composePrepare[Int](Set(_)))
  }

  object CombineByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .combineByKey(Set(_))(_ + _)(_ ++ _)
  }

  // ===== GroupByKey =====

  // 100M items, 10K keys, average 10K values per key
  object GroupByKey extends Benchmark(shuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).groupBy(_ => Random.nextInt(10 * K)).values.map(_.size)
  }

  // 10M items, 1 key
  object GroupAll extends Benchmark(shuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 10 * M).groupBy(_ => 0).values.map(_.size)
  }

  // ===== Join =====

  // LHS: 100M items, 10M keys, average 10 values per key
  // RHS: 50M items, 5M keys, average 10 values per key
  object Join extends Benchmark(shuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 10 * M) join randomKVs(sc, 50 * M, 5 * M)
  }

  // LHS: 100M items, 10M keys, average 1 values per key
  // RHS: 50M items, 5M keys, average 1 values per key
  object JoinOne extends Benchmark(shuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 100 * M) join randomKVs(sc, 50 * M, 50 * M)
  }

  // LHS: 100M items, 10M keys, average 10 values per key
  // RHS: 1M items, 100K keys, average 10 values per key
  object HashJoin extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 10 * M) hashJoin randomKVs(sc, M, 100 * K)
  }

  // ===== SideInput =====

  // Main: 100M, side: 1M

  object SingletonSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = randomUUIDs(sc, 1 * M).map(Set(_)).sum.asSingletonSideInput
      main.withSideInputs(side).map { case (x, s) => (x, s(side).size) }
    }
  }

  object IterableSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = randomUUIDs(sc, 1 * M).asIterableSideInput
      main.withSideInputs(side).map { case (x, s) => (x, s(side).head) }
    }
  }

  object ListSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = randomUUIDs(sc, 1 * M).asListSideInput
      main.withSideInputs(side)
        .map { case (x, s) => (x, s(side).head) }
    }
  }

  // Main: 1M, side: 100K

  object MapSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 1 * M)
      val side = main
        .sample(withReplacement = false, 0.1)
        .map((_, UUID.randomUUID().toString))
        .asMapSideInput
      main.withSideInputs(side).map { case (x, s) => s(side).get(x) }
    }
  }

  object MultiMapSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 1 * M)
      val side = main
        .sample(withReplacement = false, 0.1)
        .map((_, UUID.randomUUID().toString))
        .asMultiMapSideInput
      main.withSideInputs(side).map { case (x, s) => s(side).get(x) }
    }
  }

  // =======================================================================
  // Utilities
  // =======================================================================

  private val M = 1000000
  private val K = 1000

  final case class Elem[T](elem: T)

  def partitions(n: Long,
                 numPartitions: Int = 100,
                 numOfWorkers: Int = numOfWorkers): Iterable[Iterable[Long]] = {
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

  private def randomUUIDs(sc: ScioContext, n: Long): SCollection[Elem[String]] =
    sc.parallelize(partitions(n))
      .flatten
      .applyTransform(ParDo.of(new FillDoFn(() => UUID.randomUUID().toString)))
      .map(Elem(_))

  private def randomKVs(sc: ScioContext,
                        n: Long, numUniqueKeys: Int): SCollection[(String, Elem[String])] =
    sc.parallelize(partitions(n))
      .flatten
      .applyTransform(ParDo.of(new FillDoFn(() =>
        ("key" + Random.nextInt(numUniqueKeys), UUID.randomUUID().toString)
      )))
      .mapValues(Elem(_))

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
// scalastyle:on number.of.methods
// scalastyle:on number.of.types
