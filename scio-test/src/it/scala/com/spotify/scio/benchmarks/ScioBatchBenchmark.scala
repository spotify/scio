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

import com.spotify.scio._
import com.twitter.algebird.Aggregator

/**
 * Batch benchmark jobs, run once daily and logged to DataStore.
 *
 * This file is symlinked to scio-bench/src/main/scala/com/spotify/ScioBatchBenchmark.scala so
 * that it can run with past Scio releases.
 */
// scalastyle:off number.of.methods
object ScioBatchBenchmark {
  import Benchmark._
  import ScioBenchmarkSettings._

  def main(args: Array[String]): Unit =
    BenchmarkRunner.runParallel(args, "ScioBenchmark", Benchmarks)

  // =======================================================================
  // Benchmarks
  // =======================================================================

  private val Benchmarks = ScioBenchmarkSettings
    .benchmarks("com\\.spotify\\.scio\\.benchmarks\\.ScioBatchBenchmark\\$[\\w]+\\$")

  val BenchmarkNames: Seq[String] = Benchmarks.sortBy(_.name).flatMap { b =>
    b.name :: Option(b.extraConfs).getOrElse(Map.empty).keys.map(b.name + _).toList
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
      randomUUIDs(sc, 100 * M)
        .map(_.hashCode % 1000)
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
      randomUUIDs(sc, 100 * M)
        .transform("Assign random key")(withRandomKey[Elem[String]](10 * K))
        .mapValues(_.hashCode % 1000)
        .mapValues(Set(_))
        .reduceByKey(_ ++ _)
  }

  object SumByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M)
        .transform("Assign random key")(withRandomKey[Elem[String]](10 * K))
        .mapValues(_.hashCode % 1000)
        .mapValues(Set(_))
        .sumByKey
  }

  object FoldByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M)
        .transform("Assign random key")(withRandomKey[Elem[String]](10 * K))
        .mapValues(_.hashCode % 1000)
        .mapValues(Set(_))
        .foldByKey(Set.empty[Int])(_ ++ _)
  }

  object FoldByKeyMonoid extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M)
        .transform("Assign random key")(withRandomKey[Elem[String]](10 * K))
        .mapValues(_.hashCode % 1000)
        .mapValues(Set(_))
        .foldByKey
  }

  object AggregateByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M)
        .transform("Assign random key")(withRandomKey[Elem[String]](10 * K))
        .mapValues(_.hashCode % 1000)
        .aggregateByKey(Set.empty[Int])(_ + _, _ ++ _)
  }

  object AggregateByKeyAggregator extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M)
        .transform("Assign random key")(withRandomKey[Elem[String]](10 * K))
        .mapValues(_.hashCode % 1000)
        .aggregateByKey(Aggregator.fromMonoid[Set[Int]].composePrepare[Int](Set(_)))
  }

  object CombineByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M)
        .transform("Assign random key")(withRandomKey[Elem[String]](10 * K))
        .mapValues(_.hashCode % 1000)
        .combineByKey(Set(_))(_ + _)(_ ++ _)
  }

  // ===== GroupByKey =====

  // 100M items, 10K keys, average 10K values per key
  object GroupByKey extends Benchmark(ShuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M)
        .transform("Assign random key")(withRandomKey[Elem[String]](10 * K))
        .groupByKey
        .values
        .map(_.size)
  }

  // 10M items, 1 key
  object GroupAll extends Benchmark(ShuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 10 * M).groupBy(_ => 0).values.map(_.size)
  }

  // ===== Join =====

  // LHS: 100M items, 10M keys, average 10 values per key
  // RHS: 50M items, 5M keys, average 10 values per key
  object Join extends Benchmark(ShuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 10 * M) join randomKVs(sc, 50 * M, 5 * M)
  }

  // LHS: 100M items, 10M keys, average 1 values per key
  // RHS: 50M items, 5M keys, average 1 values per key
  object JoinOne extends Benchmark(ShuffleConf) {
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
      main
        .withSideInputs(side)
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

  private val M = 1000000
  private val K = 1000
}
