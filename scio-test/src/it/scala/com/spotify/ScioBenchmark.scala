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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataflow.{Dataflow, DataflowScopes}
import com.google.common.reflect.ClassPath
import com.google.datastore.v1._
import com.google.datastore.v1.client.DatastoreHelper
import com.spotify.scio._
import com.spotify.scio.runners.dataflow.DataflowResult
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Aggregator
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat, PeriodFormat}
import org.joda.time.{DateTimeZone, Instant, Seconds}
import shapeless.datatype.datastore._

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

// This file is symlinked to scio-bench/src/main/scala/com/spotify/ScioBenchmark.scala so that it
// can run with past Scio releases.

object ScioBenchmarkSettings {
  val defaultProjectId: String = "data-integration-test"
  val numOfWorkers = 4
  val commonArgs = Array(
    "--runner=DataflowRunner",
    s"--numWorkers=$numOfWorkers",
    "--workerMachineType=n1-standard-4",
    "--autoscalingAlgorithm=NONE")

  val shuffleConf = Map("ShuffleService" -> Array("--experiments=shuffle_mode=service"))
}

// scalastyle:off number.of.types
// scalastyle:off number.of.methods
object ScioBenchmark {

  import ScioBenchmarkSettings._

  case class CircleCIEnv(buildNum: Long, gitHash: String)
  case class ScioBenchmarkRun(timestamp: Instant,
                              gitHash: String, buildNum: Long, operation: String)
  case class OperationBenchmark(opName: String, metrics: Map[String, String])

  private val dataflow = {
    val transport = GoogleNetHttpTransport.newTrustedTransport()
    val jackson = JacksonFactory.getDefaultInstance
    val credential = GoogleCredential
      .getApplicationDefault.createScoped(Seq(DataflowScopes.CLOUD_PLATFORM).asJava)
    new Dataflow.Builder(transport, jackson, credential).build()
  }

  private val datastore = DatastoreHelper.getDatastoreFromEnv

  private val datastoreKind = "Benchmarks"

  private lazy val datastoreMetricKeys = Set("Elapsed", "TotalMemoryUsage", "TotalPdUsage",
    "TotalShuffleDataProcessed", "TotalSsdUsage", "TotalStreamingDataProcessed", "TotalVcpuTime")

  private val circleCIEnv: Option[CircleCIEnv] = {
    val isCircleCIRun = sys.env.get("CIRCLECI").contains("true")

    if (isCircleCIRun) {
      (sys.env.get("CIRCLE_BUILD_NUM"), sys.env.get("CIRCLE_SHA1")) match {
        case (Some(buildNumber), Some(gitHash)) =>
          Some(CircleCIEnv(buildNumber.toLong, gitHash))
        case _ => throw new IllegalStateException("CIRCLECI env variable is set but not " +
          "CIRCLE_BUILD_NUM and CIRCLE_SHA1")
      }
    } else {
      prettyPrint("CircleCI", "CIRCLECI env variable not found. Will not publish " +
        "benchmark results to Datastore.")
      None
    }
  }

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
      .flatMap(_.run(projectId, prefix))

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future.sequence(results.map(_.result.finalState))
    Await.result(future, Duration.Inf)

    // scalastyle:off regex
    val metrics = results.map { r =>
      println("=" * 80)
      prettyPrint("Benchmark", r.name)
      prettyPrint("Extra arguments", r.extraArgs.mkString(" "))
      prettyPrint("State", r.result.state.toString)

      val jobId = r.result.internal.asInstanceOf[DataflowPipelineJob].getJobId
      val job = dataflow.projects().jobs().get(projectId, jobId).setView("JOB_VIEW_ALL").execute()
      val parser = ISODateTimeFormat.dateTimeParser()
      prettyPrint("Create time", job.getCreateTime)
      prettyPrint("Finish time", job.getCurrentStateTime)
      val start = parser.parseLocalDateTime(job.getCreateTime)
      val finish = parser.parseLocalDateTime(job.getCurrentStateTime)
      val elapsed = Seconds.secondsBetween(start, finish)
      prettyPrint("Elapsed", PeriodFormat.getDefault.print(elapsed))

      val jobMetrics = r.result.as[DataflowResult].getJobMetrics.getMetrics.asScala
        .filter { m =>
          m.getName.getName.startsWith("Total") && !m.getName.getContext.containsKey("tentative")
        }
        .map(m => (m.getName.getName, m.getScalar.toString))
        .sortBy(_._1)

      jobMetrics.foreach(kv => prettyPrint(kv._1, kv._2))
      OperationBenchmark(r.name,
        jobMetrics.filter(metric => datastoreMetricKeys.contains(metric._1)).toMap +
          ("Elapsed" -> elapsed.getSeconds.toString))
    }
    if (circleCIEnv.isDefined) {
      saveMetricsToDatastore(circleCIEnv.get, metrics)
    }
    // scalastyle:on regex
  }

  // scalastyle:off regex
  // Save metrics to integration testing Datastore instance. Can't make this into a
  // transaction because DS limit is 25 entities per transaction.
  private def saveMetricsToDatastore(circleCIEnv: CircleCIEnv,
                                     benchmarks: Iterable[OperationBenchmark]): Unit = {
    prettyPrint("CircleCI", s"Saving metrics for $circleCIEnv to Datastore...")

    val now = new Instant()
    val dt = DatastoreType[ScioBenchmarkRun]

    benchmarks.foreach { benchmark =>
      val entity = dt
        .toEntityBuilder(
          ScioBenchmarkRun(now, circleCIEnv.gitHash, circleCIEnv.buildNum, benchmark.opName))
        .setKey(DatastoreHelper.makeKey(
          s"${datastoreKind}_${benchmark.opName}", circleCIEnv.buildNum.toString))

      benchmark.metrics.foreach { metric =>
        entity.putProperties(metric._1, DatastoreHelper.makeValue(metric._2).build())
      }

      try {
        datastore.commit(CommitRequest.newBuilder()
          .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
          // Upsert means we can re-run a job for same build if necessary; insert would trigger
          // a Datastore exception
          .addMutations(Mutation.newBuilder().setUpsert(entity.build()).build())
          .build())
      } catch {
        case e: Exception =>
          println("Caught exception committing to Datastore. Metrics may not have been " +
            s"published for operation ${benchmark.opName}")
          e.printStackTrace()
      }
    }

    printMetricsComparison(benchmarks.map(_.opName))
  }

  private val getBenchmarkQuery =
    s"SELECT * from ${datastoreKind}_%s ORDER BY buildNum DESC LIMIT 2"

  // TODO: move this to email generator
  private def printMetricsComparison(benchmarkNames: Iterable[String]): Unit = {
    benchmarkNames.foreach { benchmarkName =>
      try {
        val comparisonMetrics = datastore.runQuery(
          RunQueryRequest.newBuilder().setGqlQuery(
            GqlQuery.newBuilder()
              .setAllowLiterals(true)
              .setQueryString(getBenchmarkQuery.format(benchmarkName))
              .build()
          ).build())

        val metrics = comparisonMetrics.getBatch.getEntityResultsList.asScala
          .sortBy(_.getEntity.getKey.getPath(0).getName.toInt)
          .map(_.getEntity)
        if (metrics.size == 2) {
          val opName = metrics.head.getKey.getPath(0).getKind.substring(datastoreKind.length + 1)
          val props = metrics.map(_.getPropertiesMap.asScala)
          println("=" * 80)
          prettyPrint("Benchmark", opName)
          val List(b1, b2) = props.map(_("buildNum").getIntegerValue).toList
          prettyPrint("BuildNum", "%15d%15d%15s".format(b1, b2, "Delta"))
          datastoreMetricKeys.foreach { k =>
            val List(prev, curr) = props.map(_(k).getStringValue.toDouble).toList
            val delta = (curr - prev).toDouble / curr * 100.0
            val signed = if (delta.isNaN) {
              "0.00%"
            } else {
              (if (delta > 0) "+" else "") + "%.2f%%".format(delta)
            }
            prettyPrint(k, "%15.2f%15.2f%15s".format(prev, curr, signed))
          }
        }
      } catch {
        case e: Exception => println(s"Caught error fetching benchmark metrics from Datastore: $e")
      }
    }
  }
  // scalastyle:on regex

  private def prettyPrint(k: String, v: String): Unit = {
    // scalastyle:off regex
    println("%-30s: %s".format(k, v))
    // scalastyle:on regex
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

  case class BenchmarkResult(name: String, extraArgs: Array[String], result: ScioResult)

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

    def run(projectId: String, prefix: String): Iterable[BenchmarkResult] = {
      val username = sys.props("user.name")
      configurations
        .map { case (confName, extraArgs) =>
          val (sc, _) = ContextAndArgs(Array(s"--project=$projectId") ++ commonArgs ++ extraArgs)
          sc.setAppName(confName)
          sc.setJobName(s"$prefix-$confName-$username".toLowerCase())
          run(sc)
          BenchmarkResult(confName, extraArgs, sc.close())
        }
    }

    def run(sc: ScioContext): Unit
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
