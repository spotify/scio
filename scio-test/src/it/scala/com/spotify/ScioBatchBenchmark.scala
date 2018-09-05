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

package com.spotify

import java.util.UUID

import com.google.common.reflect.ClassPath
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Aggregator
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeZone

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.higherKinds
import scala.util.{Random, Try}

/**
 * Batch benchmark jobs, run once daily and logged to DataStore.
 *
 * This file is symlinked to scio-bench/src/main/scala/com/spotify/ScioBatchBenchmark.scala so
 * that it can run with past Scio releases.
 */
object ScioBatchBenchmark {
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
      .flatMap(_.run(projectId, prefix, commonArgs()))

    val logger = ScioBenchmarkLogger[Try](
      ConsoleLogger(),
      new DatastoreLogger(BatchMetrics)
    )

    val future = Future.sequence(results.map(_.map(logger.log(_))))
    Await.result(future, Duration.Inf)
  }

  // =======================================================================
  // Benchmarks
  // =======================================================================

  private val benchmarks = ClassPath.from(Thread.currentThread().getContextClassLoader)
    .getAllClasses
    .asScala
    .filter(_.getName.matches("com\\.spotify\\.ScioBatchBenchmark\\$[\\w]+\\$"))
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

  def run(projectId: String,
          prefix: String,
          args: Array[String]): Iterable[Future[BenchmarkResult]] = {
    val username = sys.props("user.name")
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

  def run(sc: ScioContext): Unit
}
