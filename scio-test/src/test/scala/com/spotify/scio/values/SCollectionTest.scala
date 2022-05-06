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

package com.spotify.scio.values

import java.io.PrintStream
import java.nio.file.Files
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.MockedPrintStream
import com.spotify.scio.util.random.RandomSamplerUtils
import com.twitter.algebird.{Aggregator, Semigroup}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{Count, DoFn, GroupByKey, ParDo}
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing
import org.apache.beam.sdk.transforms.windowing.{
  BoundedWindow,
  GlobalWindow,
  IntervalWindow,
  PaneInfo
}
import org.apache.beam.sdk.values.KV
import org.joda.time.{DateTimeConstants, Duration, Instant}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import com.spotify.scio.coders.Coder
import com.spotify.scio.schemas.Schema

import java.nio.charset.StandardCharsets

class SCollectionTest extends PipelineSpec {

  "SCollection" should "support applyTransform()" in {
    runWithContext { sc =>
      val p =
        sc.parallelize(Seq(1, 2, 3, 4, 5)).applyTransform(Count.globally())
      p should containSingleValue(5L.asInstanceOf[java.lang.Long])
    }
  }

  private def newKvDoFn = new DoFn[Int, KV[Int, String]] {
    @ProcessElement
    def processElement(c: DoFn[Int, KV[Int, String]]#ProcessContext): Unit = {
      val x = c.element()
      c.output(KV.of(x, x.toString))
    }
  }

  it should "set schema" in {
    runWithContext { sc =>
      val empty = sc.empty[(Int, Int)]()
      empty.keyBy(_._1)

      empty.setSchema(Schema[(Int, Int)])
    }
  }

  it should "support applyKvTransform()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(1, 2, 3, 4, 5))
        .applyKvTransform(ParDo.of(newKvDoFn))
        .applyKvTransform(GroupByKey.create())
        .map(kv => (kv.getKey, kv.getValue.asScala.toList))
      p should containInAnyOrder(Seq(1, 2, 3, 4, 5).map(x => (x, List(x.toString))))
    }
  }

  it should "support transform()" in {
    runWithContext {
      _.parallelize(1 to 10)
        .transform(_.map(_ * 10).sum) should containSingleValue(550)
    }
  }

  it should "support unionAll()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq("d", "e", "f"))
      val p3 = sc.parallelize(Seq("g", "h", "i"))
      val r = SCollection.unionAll(Seq(p1, p2, p3))
      val expected = Seq("a", "b", "c", "d", "e", "f", "g", "h", "i")
      r should containInAnyOrder(expected)
    }
  }

  it should "support unionAll() when called on ScioContext" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq("d", "e", "f"))
      val p3 = sc.parallelize(Seq("g", "h", "i"))
      val r = sc.unionAll(Seq(p1, p2, p3))
      val expected = Seq("a", "b", "c", "d", "e", "f", "g", "h", "i")
      r should containInAnyOrder(expected)
    }
  }

  it should "support unionAll() with an empty list" in {
    runWithContext(sc => sc.unionAll(List[SCollection[Unit]]()) should beEmpty)
  }

  it should "support unionAll() with named transforms" in {
    val sc = ScioContext.forTest()

    noException shouldBe thrownBy {
      sc
        .withName("Test Union Name")
        .unionAll(
          Seq(
            sc
              .withName("Input A")
              .parallelize(1 to 10),
            sc
              .withName("Input B")
              .parallelize(11 to 20)
          )
        )
    }
  }

  it should "support ++ operator" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq("d", "e", "f"))
      val r1 = p1 ++ p2
      val r2 = p1.union(p2)
      val expected = Seq("a", "b", "c", "d", "e", "f")
      r1 should containInAnyOrder(expected)
      r2 should containInAnyOrder(expected)
    }
  }

  it should "support ++ operator with duplicates" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c", "a", "d"))
      val p2 = sc.parallelize(Seq("d", "e", "f", "a", "d"))
      val r1 = p1 ++ p2
      val r2 = p1.union(p2)
      val expected = Seq("a", "a", "a", "b", "c", "d", "d", "d", "e", "f")
      r1 should containInAnyOrder(expected)
      r2 should containInAnyOrder(expected)
    }
  }

  it should "support intersection()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
      val p2 = sc.parallelize(Seq(2, 4, 6, 8, 10))
      val p = p1.intersection(p2)
      p should containInAnyOrder(Seq(2, 4))
    }
  }

  it should "support intersection() with duplicates" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 2, 4))
      val p2 = sc.parallelize(Seq(2, 4, 6, 8, 10, 2, 4))
      val p = p1.intersection(p2)
      p should containInAnyOrder(Seq(2, 4))
    }
  }

  it should "support partition()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5, 6)).partition(2, _ % 2)
      p(0) should containInAnyOrder(Seq(2, 4, 6))
      p(1) should containInAnyOrder(Seq(1, 3, 5))
    }
  }

  it should "support partition() according to a predicate" in {
    runWithContext { sc =>
      val (p1, p2) = sc.parallelize(Seq(1, 2, 3, 4, 5, 6)).partition(_ % 2 == 0)
      p1 should containInAnyOrder(Seq(2, 4, 6))
      p2 should containInAnyOrder(Seq(1, 3, 5))
    }
  }

  it should "support partitionByKey()" in {
    runWithContext { sc =>
      val m = sc
        .parallelize(Seq("a1", "a2", "a3", "b4", "b5", "c6"))
        .partitionByKey(Set("a", "b", "c"))(_.substring(0, 1))

      m("a") should containInAnyOrder(Seq("a1", "a2", "a3"))
      m("b") should containInAnyOrder(Seq("b4", "b5"))
      m("c") should containInAnyOrder(Seq("c6"))
    }
  }

  it should "support hashPartition() based on Object.hashCode()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(-1, 2, -3, 4, -5, 6)).hashPartition(3)
      p(0) should containInAnyOrder(Seq(-3, 6))
      p(1) should containInAnyOrder(Seq(4, -5))
      p(2) should containInAnyOrder(Seq(-1, 2))
    }
  }

  it should "support aggregate()" in {
    runWithContext { sc =>
      val p = sc.parallelize(1 to 100)
      val p1 = p.aggregate(0.0)(_ + _, _ + _)
      val p2 = p.aggregate(Aggregator.max[Int])
      val p3 = p.aggregate(Aggregator.immutableSortedReverseTake[Int](5))
      p1 should containSingleValue(5050.0)
      p2 should containSingleValue(100)
      p3 should containSingleValue(Seq(100, 99, 98, 97, 96))
    }
  }

  it should "support aggregate() with mutation" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(1 to 100)
        .aggregate(mutable.Buffer.empty[Int])(
          (xs, x) => {
            xs.append(x)
            xs
          },
          (xs, ys) => {
            xs.appendAll(ys)
            xs
          }
        )
        .map(_.sorted)
      p should containSingleValue(mutable.Buffer(1 to 100: _*))
    }
  }

  it should "support collect" in {
    runWithContext { sc =>
      val records = Seq(
        ("test1", 1),
        ("test2", 2),
        ("test3", 3)
      )
      val p = sc.parallelize(records).collect { case ("test2", x) => 2 * x }
      p should containSingleValue(4)
    }
  }

  it should "support combine()" in {
    runWithContext { sc =>
      val p = sc.parallelize(1 to 100).combine(_.toDouble)(_ + _)(_ + _)
      p should containSingleValue(5050.0)
    }
  }

  it should "support combine() with mutation" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(1 to 100)
        .combine(mutable.Buffer(_)) { (xs, x) =>
          xs.append(x)
          xs
        } { (xs, ys) =>
          xs.appendAll(ys)
          xs
        }
        .map(_.sorted)
      p should containSingleValue(mutable.Buffer(1 to 100: _*))
    }
  }

  it should "support count" in {
    runWithContext(sc => sc.parallelize(Seq("a", "b", "c")).count should containSingleValue(3L))
  }

  it should "support countApproxDistinct()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("a", "b", "b", "c", "c", "c"))
      val r1 = p.countApproxDistinct()
      val r2 = p.countApproxDistinct(sampleSize = 10000)
      r1 should containSingleValue(3L)
      r2 should containSingleValue(3L)
    }
  }

  it should "support countByValue" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("a", "b", "b", "c", "c", "c")).countByValue
      p should containInAnyOrder(Seq(("a", 1L), ("b", 2L), ("c", 3L)))
    }
  }

  it should "support distinct on String" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("a", "b", "b", "c", "c", "c")).distinct
      p should containInAnyOrder(Seq("a", "b", "c"))
    }
  }

  it should "support distinct on Int" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 3, 4, 2, 2, 5, 4, 2, 1, 1)).distinct
      p should containInAnyOrder(Seq(1, 2, 3, 4, 5))
    }
  }

  it should "support distinctBy() with Scala primitive as representative values" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(1 -> "vA1", 2 -> "vB", 1 -> "vA2"))
        .distinctBy(_._1)
      p.keys should containInAnyOrder(Seq(1, 2))
    }
  }

  it should "support filter()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5)).filter(_ % 2 == 0)
      p should containInAnyOrder(Seq(2, 4))
    }
  }

  it should "support filterNot()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5)).filterNot(_ % 2 == 0)
      p should containInAnyOrder(Seq(1, 3, 5))
    }
  }

  it should "support flatMap()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("a b c", "d e", "f")).flatMap(_.split(" "))
      p should containInAnyOrder(Seq("a", "b", "c", "d", "e", "f"))
    }
  }

  it should "support flatten()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(Seq("a b", "c d"), Seq("e f", "g h"))).flatten
      p1 should containInAnyOrder(Seq("a b", "c d", "e f", "g h"))

      val p2 = sc.parallelize(Seq(Some(1), None)).flatten
      p2 should containInAnyOrder(Seq(1))
    }
  }

  it should "support fold()" in {
    runWithContext { sc =>
      val p = sc.parallelize(1 to 100)
      val r1 = p.fold(0)(_ + _)
      val r2 = p.fold
      r1 should containSingleValue(5050)
      r2 should containSingleValue(5050)
    }
  }

  it should "support fold() with mutation" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(1 to 100)
        .map(mutable.Buffer(_))
        .fold(mutable.Buffer.empty[Int]) { (xs, ys) =>
          xs.appendAll(ys)
          xs
        }
        .map(_.sorted)
      p should containSingleValue(mutable.Buffer(1 to 100: _*))
    }
  }

  it should "support groupBy()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4)).groupBy(_ % 2).mapValues(_.toSet)
      p should containInAnyOrder(Seq((0, Set(2, 4)), (1, Set(1, 3))))
    }
  }

  it should "support groupMap()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2)).groupMap(_ + 1)(_ + 1)
      p should containInAnyOrder(Seq((2, Iterable(2)), (3, Iterable(3))))
    }
  }

  it should "support groupMapReduce()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4)).groupMapReduce(_ % 2)(_ + _)
      p should containInAnyOrder(Seq((0, 6), (1, 4)))
    }
  }

  it should "support keyBy()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("hello", "world")).keyBy(_.substring(0, 1))
      p should containInAnyOrder(Seq(("h", "hello"), ("w", "world")))
    }
  }

  it should "support map()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("1", "2", "3")).map(_.toInt)
      p should containInAnyOrder(Seq(1, 2, 3))
    }
  }

  it should "support max" in {
    runWithContext { sc =>
      def max[T: Coder: Numeric](elems: T*): SCollection[T] =
        sc.parallelize(elems).max
      max(1, 2, 3) should containSingleValue(3)
      max(1L, 2L, 3L) should containSingleValue(3L)
      max(1f, 2f, 3f) should containSingleValue(3f)
      max(1.0, 2.0, 3.0) should containSingleValue(3.0)
    }
  }

  it should "support mean" in {
    runWithContext { sc =>
      def mean[T: Coder: Numeric](elems: T*): SCollection[Double] =
        sc.parallelize(elems).mean
      mean(1, 2, 3) should containSingleValue(2.0)
      mean(1L, 2L, 3L) should containSingleValue(2.0)
      mean(1f, 2f, 3f) should containSingleValue(2.0)
      mean(1.0, 2.0, 3.0) should containSingleValue(2.0)
    }
  }

  it should "support min" in {
    runWithContext { sc =>
      def min[T: Coder: Numeric](elems: T*): SCollection[T] =
        sc.parallelize(elems).min
      min(1, 2, 3) should containSingleValue(1)
      min(1L, 2L, 3L) should containSingleValue(1L)
      min(1f, 2f, 3f) should containSingleValue(1f)
      min(1.0, 2.0, 3.0) should containSingleValue(1.0)
    }
  }

  it should "support quantilesApprox()" in {
    runWithContext { sc =>
      val p = sc.parallelize(0 to 100).quantilesApprox(5)
      p should containSingleValue(Iterable(0, 25, 50, 75, 100))
    }
  }

  it should "support randomSplit()" in {
    runWithContext { sc =>
      val total = 5000
      def round(c: Long): Double = math.round(c * 10.0 / total) / 10.0
      val p1 = sc.parallelize(0 to total).randomSplit(Array(0.3, 0.7))
      val p2 = sc.parallelize(0 to total).randomSplit(Array(0.2, 0.3, 0.5))
      p1.length shouldBe 2
      p2.length shouldBe 3
      p1(0).count.map(round) should containSingleValue(0.3)
      p1(1).count.map(round) should containSingleValue(0.7)
      p2(0).count.map(round) should containSingleValue(0.2)
      p2(1).count.map(round) should containSingleValue(0.3)
      p2(2).count.map(round) should containSingleValue(0.5)

      val (pa, pb) = sc.parallelize(0 to 5000).randomSplit(0.3)
      val (pc, pd, pe) = sc.parallelize(0 to 5000).randomSplit(0.2, 0.3)
      pa.count.map(round) should containSingleValue(0.3)
      pb.count.map(round) should containSingleValue(0.7)
      pc.count.map(round) should containSingleValue(0.2)
      pd.count.map(round) should containSingleValue(0.3)
      pe.count.map(round) should containSingleValue(0.5)
    }
  }

  it should "support reduce()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5)).reduce(_ + _)
      p should containSingleValue(15)
    }
  }

  it should "support sample()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 1, 1, 1, 1))
      val r1 = p.sample(1)
      val r2 = p.sample(5)
      r1 should containSingleValue(Iterable(1))
      r2 should containSingleValue(Iterable(1, 1, 1, 1, 1))
    }
  }

  it should "support sample() with replacement" in {
    import RandomSamplerUtils._
    for (fraction <- List(0.05, 0.2, 1.0)) {
      val sample = runWithData(population)(_.sample(true, fraction))
      (sample.size.toDouble / populationSize) shouldBe fraction +- 0.05
      sample.toSet.size should be < sample.size
    }
  }

  it should "support sample() without replacement" in {
    import RandomSamplerUtils._
    for (fraction <- List(0.05, 0.2, 1.0)) {
      val sample = runWithData(population)(_.sample(false, fraction))
      (sample.size.toDouble / populationSize) shouldBe fraction +- 0.05
      sample.toSet.size shouldBe sample.size
    }
  }

  it should "support subtract()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
      val p2 = sc.parallelize(Seq(2, 4, 6, 8, 10))
      val p = p1.subtract(p2)
      p should containInAnyOrder(Seq(1, 3, 5))
    }
  }

  it should "support subtract() with duplicates" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 1, 3, 5))
      val p2 = sc.parallelize(Seq(2, 4, 6, 8, 10))
      val p = p1.subtract(p2)
      p should containInAnyOrder(Seq(1, 3, 5, 1, 3, 5))
    }
  }

  it should "support sum" in {
    runWithContext { sc =>
      def sum[T: Coder: Semigroup](elems: T*): SCollection[T] =
        sc.parallelize(elems).sum
      sum(1, 2, 3) should containSingleValue(6)
      sum(1L, 2L, 3L) should containSingleValue(6L)
      sum(1f, 2f, 3f) should containSingleValue(6f)
      sum(1.0, 2.0, 3.0) should containSingleValue(6.0)
      sum(1 to 100: _*) should containSingleValue(5050)
    }
  }

  it should "support take()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
      p.take(1) should haveSize(1)
      p.take(2) should haveSize(2)
    }
  }

  it should "support top()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
      val r1 = p.top(3)
      val r2 = p.top(3)(Ordering.by(-_))
      r1 should containSingleValue(Iterable(5, 4, 3))
      r2 should containSingleValue(Iterable(1, 2, 3))
    }
  }

  it should "support cross()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1))
      val p3 = sc.parallelize(Seq(1, 2))
      val s1 = p1.cross(p2)
      val s2 = p1.cross(p3)
      s1 should containInAnyOrder(Seq(("a", 1), ("b", 1), ("c", 1)))
      s2 should containInAnyOrder(Seq(("a", 1), ("a", 2), ("b", 1), ("b", 2), ("c", 1), ("c", 2)))
    }
  }

  it should "support hashLookup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(("a", 1), ("b", 2), ("b", 3)))
      val p = p1.hashLookup(p2).mapValues(_.toSet)
      p should containInAnyOrder(Seq(("a", Set(1)), ("b", Set(2, 3)), ("c", Set[Int]())))
    }
  }

  it should "support withFixedWindows()" in {
    runWithContext { sc =>
      val p =
        sc.parallelizeTimestamped(
          Seq("a", "b", "c", "d", "e", "f"),
          (0L to 5L).map(new Instant(_))
        )
      val r = p.withFixedWindows(Duration.millis(3)).top(10).map(_.toSet)
      r should containInAnyOrder(Seq(Set("a", "b", "c"), Set("d", "e", "f")))
    }
  }

  it should "support withSlidingWindows()" in {
    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(
        Seq("a", "b", "c", "d", "e", "f"),
        (0L to 5L).map(new Instant(_))
      )
      val r = p
        .withSlidingWindows(Duration.millis(2), Duration.millis(2))
        .top(10)
        .map(_.toSet)
      r should containInAnyOrder(Seq(Set("a", "b"), Set("c", "d"), Set("e", "f")))
    }
  }

  it should "#2745: not throw an exception for valid values" in runWithContext { sc =>
    val p = sc.parallelizeTimestamped(
      Seq("a", "b", "c", "d", "e", "f"),
      (0L to 5L).map(new Instant(_))
    )
    val r = p
      .withSlidingWindows(
        size = Duration.standardHours(24),
        period = Duration.standardHours(25),
        offset = Duration.standardHours(23)
      )
      .top(10)
      .map(_.toSet)
    r shouldNot beEmpty
  }

  it should "support withSessionWindows()" in {
    runWithContext { sc =>
      val p =
        sc.parallelizeTimestamped(
          Seq("a", "b", "c", "d", "e"),
          Seq(0L, 5L, 10L, 44L, 55L).map(new Instant(_))
        )
      val r = p
        .withSessionWindows(Duration.millis(10))
        .top(10)
        .map(_.toSet)
      r should containInAnyOrder(Seq(Set("a", "b", "c"), Set("d"), Set("e")))
    }
  }

  it should "support withGlobalWindow()" in {
    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(
        Seq("a", "b", "c", "d", "e", "f"),
        (0L to 5L).map(new Instant(_))
      )
      val r = p
        .withFixedWindows(Duration.millis(3))
        .withGlobalWindow()
        .top(10)
        .map(_.toSet)
      r should containInAnyOrder(Seq(Set("a", "b", "c", "d", "e", "f")))
    }
  }

  it should "support withPaneInfo" in {
    runWithContext { sc =>
      val pane = PaneInfo.createPane(true, true, Timing.UNKNOWN, 0, 0)
      val p = sc.parallelizeTimestamped(
        Seq("a", "b", "c"),
        Seq(1L, 2L, 3L).map(new Instant(_))
      )
      val r = p.withPaneInfo.map(kv => (kv._1, kv._2))
      r should containInAnyOrder(Seq(("a", pane), ("b", pane), ("c", pane)))
    }
  }

  it should "support withTimestamp" in {
    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(
        Seq("a", "b", "c"),
        Seq(1L, 2L, 3L).map(new Instant(_))
      )
      val r = p.withTimestamp.map(kv => (kv._1, kv._2.getMillis))
      r should containInAnyOrder(Seq(("a", 1L), ("b", 2L), ("c", 3L)))
    }
  }

  it should "support withWindow" in {
    def w2s(window: BoundedWindow): String = window match {
      case w: GlobalWindow   => s"GlobalWindow(${w.maxTimestamp()})"
      case w: IntervalWindow => s"IntervalWindow(${w.start()}, ${w.end()})"
      case _                 => ???
    }

    val timestamps = Seq(1L, 2L, 3L).map(t => new Instant(t * DateTimeConstants.MILLIS_PER_MINUTE))

    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(Seq("a", "b", "c"), timestamps)
      val r = p.withWindow[BoundedWindow].map(kv => (kv._1, w2s(kv._2)))

      // default is GlobalWindow
      val w = w2s(GlobalWindow.INSTANCE)
      r should containInAnyOrder(Seq(("a", w), ("b", w), ("c", w)))
    }

    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(Seq("a", "b", "c"), timestamps)
      val r = p.withWindow[GlobalWindow].map(kv => (kv._1, w2s(kv._2)))

      // default is GlobalWindow
      val w = w2s(GlobalWindow.INSTANCE)
      r should containInAnyOrder(Seq(("a", w), ("b", w), ("c", w)))
    }

    runWithContext { sc =>
      val p = sc
        .parallelizeTimestamped(Seq("a", "b", "c"), timestamps)
        .withFixedWindows(Duration.standardMinutes(1))
      val r = p.withWindow[IntervalWindow].map(kv => (kv._1, w2s(kv._2)))

      // type is IntervalWindow after windowing is applied
      val ws = timestamps.map(t => w2s(new IntervalWindow(t, t.plus(Duration.standardMinutes(1)))))
      r should containInAnyOrder(Seq("a", "b", "c").zip(ws))
    }
  }

  it should "support timestampBy()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      val r = p
        .timestampBy(t => new Instant(t.toLong))
        .withTimestamp
        .map(kv => (kv._1, kv._2.getMillis))
      r should containInAnyOrder(Seq((1, 1L), (2, 2L), (3, 3L)))
    }
  }

  it should "support timestampBy() with skew" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      val r = p
        .timestampBy(t => new Instant(t.toLong), Duration.millis(1))
        .withTimestamp
        .map(kv => (kv._1, kv._2.getMillis))
      r should containInAnyOrder(Seq((1, 1L), (2, 2L), (3, 3L)))
    }
  }

  it should "support debug to the stdout" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      runWithContext { sc =>
        val r = sc.parallelize(1 to 3).debug()
        r should containInAnyOrder(Seq(1, 2, 3))
      }
    }
    stdOutMock.message.filterNot(_ == "\n") should contain theSameElementsAs Seq("1", "2", "3")
  }

  it should "support debug to the stdout with prefix" in {
    val stdOutMock = new MockedPrintStream
    Console.withOut(stdOutMock) {
      runWithContext { sc =>
        val r = sc.parallelize(1 to 3).debug(prefix = "===")
        r should containInAnyOrder(Seq(1, 2, 3))
      }
    }
    stdOutMock.message.filterNot(_ == "\n") should contain theSameElementsAs
      Seq("===1", "===2", "===3")
  }

  it should "support debug to the stderr" in {
    val stdErrMock = new MockedPrintStream
    Console.withErr(stdErrMock) {
      runWithContext { sc =>
        val r = sc.parallelize(1 to 3).debug(() => Console.err)
        r should containInAnyOrder(Seq(1, 2, 3))
      }
    }
    stdErrMock.message.filterNot(_ == "\n") should contain theSameElementsAs Seq("1", "2", "3")
  }

  it should "support debug to a file" in {
    val outFile = Files.createTempFile("debug-test-", "txt")
    val fileStream = new PrintStream(outFile.toFile)
    Console.withOut(fileStream) {
      runWithContext { sc =>
        val r = sc.parallelize(1 to 3).debug()
        r should containInAnyOrder(Seq(1, 2, 3))
      }
    }
    Files.readAllLines(outFile, StandardCharsets.UTF_8) should contain theSameElementsAs Seq(
      "1",
      "2",
      "3"
    )
  }

  it should "support Combine.globally() with default value" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq.empty[Int])
      val p1 = p.aggregate(0)(_ + _, _ + _)
      val p2 = p.fold
      val p3 = p.fold(0)(_ + _)
      p1 should containSingleValue(0)
      p2 should containSingleValue(0)
      p3 should containSingleValue(0)
    }
  }

  it should "emit 0 instead of an empty SCollection when counting an empty SCollection" in {
    runWithContext { sc =>
      val count = sc.parallelize(List[String]()).count
      count should containSingleValue(0L)
    }
  }

  it should "reify as List" in {
    runWithContext { sc =>
      val other = sc.parallelize(Seq(1))
      val coll = sc.parallelize(Seq(1, 2)).reifySideInputAsValues(other.asListSideInput)
      coll should containInAnyOrder(Seq((1, Seq(1)), (2, Seq(1))))
    }
  }

  it should "reify as Iterable" in {
    runWithContext { sc =>
      val other = sc.parallelize(Seq(1))
      val coll = sc.parallelize(Seq(1, 2)).reifySideInputAsValues(other.asIterableSideInput)
      coll should containInAnyOrder(Seq((1, Iterable(1)), (2, Iterable(1))))
    }
  }

  it should "reify as Map" in {
    runWithContext { sc =>
      val other = sc.parallelize(Seq((1, 1)))
      val coll = sc.parallelize(Seq(1, 2)).reifySideInputAsValues(other.asMapSideInput)
      coll should containInAnyOrder(Seq((1, Map(1 -> 1)), (2, Map(1 -> 1))))
    }
  }

  it should "reify as Multimap" in {
    runWithContext { sc =>
      val other = sc.parallelize(Seq((1, 1)))
      val coll = sc.parallelize(Seq(1, 2)).reifySideInputAsValues(other.asMultiMapSideInput)
      coll should containInAnyOrder(Seq((1, Map(1 -> Iterable(1))), (2, Map(1 -> Iterable(1)))))
    }
  }

  it should "reify in Golbal Window as List" in {
    runWithContext { sc =>
      val coll = sc.parallelize(Seq(1)).reifyAsListInGlobalWindow
      coll should containInAnyOrder(Seq(Seq(1)))
    }
  }

  it should "reify empty in Golbal Window as List" in {
    runWithContext { sc =>
      val coll = sc.empty[Int]().reifyAsListInGlobalWindow
      coll should containInAnyOrder(Seq(Seq.empty[Int]))
    }
  }

}
