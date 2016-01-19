package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.Algebird
import com.twitter.algebird.{Aggregator, Semigroup}
import org.joda.time.{Duration, Instant}

import scala.reflect.ClassTag

class SCollectionTest extends PipelineSpec {

  import com.spotify.scio.testing.TestingUtils._

  "SCollection" should "support setName()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5)).setName("MySCollection")
      p.name shouldBe "MySCollection"
    }
  }

  it should "support ++ operator" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq("d", "e", "f"))
      val r1 = p1 ++ p2
      val r2 = p1.union(p2)
      val expected = Seq("a", "b", "c", "d", "e", "f")
      r1.internal should equalInAnyOrder (expected)
      r2.internal should equalInAnyOrder (expected)
    }
  }

  it should "support ++ operator with duplicates" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c", "a", "d"))
      val p2 = sc.parallelize(Seq("d", "e", "f", "a", "d"))
      val r1 = p1 ++ p2
      val r2 = p1.union(p2)
      val expected = Seq("a", "a", "a", "b", "c", "d", "d", "d", "e", "f")
      r1.internal should equalInAnyOrder (expected)
      r2.internal should equalInAnyOrder (expected)
    }
  }

  it should "support intersection()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
      val p2 = sc.parallelize(Seq(2, 4, 6, 8, 10))
      val p = p1.intersection(p2)
      p.internal should containInAnyOrder (Seq(2, 4))
    }
  }

  it should "support intersection() with duplicates" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 2, 4))
      val p2 = sc.parallelize(Seq(2, 4, 6, 8, 10, 2, 4))
      val p = p1.intersection(p2)
      p.internal should containInAnyOrder (Seq(2, 4))
    }
  }

  it should "support partition()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5, 6)).partition(2, _ % 2)
      p(0).internal should containInAnyOrder (Seq(2, 4, 6))
      p(1).internal should containInAnyOrder (Seq(1, 3, 5))
    }
  }

  it should "support aggregate()" in {
    runWithContext { sc =>
      val p = sc.parallelize(1 to 100)
      val p1 = p.aggregate(0.0)(_ + _, _ + _)
      val p2 = p.aggregate(Aggregator.max[Int])
      val p3 = p.aggregate(Algebird.sortedReverseTake[Int](5))
      p1.internal should containSingleValue (5050.0)
      p2.internal should containSingleValue (100)
      p3.internal should containSingleValue (Seq(100, 99, 98, 97, 96))
    }
  }

  it should "support combine()" in {
    runWithContext { sc =>
      val p = sc.parallelize(1 to 100).combine(_.toDouble)(_ + _)(_ + _)
      p.internal should containSingleValue (5050.0)
    }
  }

  it should "support count()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("a", "b", "c")).count()
      p.internal should containSingleValue (3L)
    }
  }

  it should "support countApproxDistinct()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("a", "b", "b", "c", "c", "c"))
      val r1 = p.countApproxDistinct()
      val r2 = p.countApproxDistinct(sampleSize = 10000)
      r1.internal should containSingleValue (3L)
      r2.internal should containSingleValue (3L)
    }
  }

  it should "support countByValue()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("a", "b", "b", "c", "c", "c")).countByValue()
      p.internal should containInAnyOrder (Seq(("a", 1L), ("b", 2L), ("c", 3L)))
    }
  }

  it should "support distinct()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("a", "b", "b", "c", "c", "c")).distinct()
      p.internal should containInAnyOrder (Seq("a", "b", "c"))
    }
  }

  it should "support filter()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5)).filter(_ % 2 == 0)
      p.internal should containInAnyOrder (Seq(2, 4))
    }
  }

  it should "support flatMap()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("a b c", "d e", "f")).flatMap(_.split(" "))
      p.internal should containInAnyOrder (Seq("a", "b", "c", "d", "e", "f"))
    }
  }

  it should "support fold()" in {
    runWithContext { sc =>
      val p = sc.parallelize(1 to 100)
      val r1 = p.fold(0)(_ + _)
      val r2 = p.fold
      r1.internal should containSingleValue (5050)
      r2.internal should containSingleValue (5050)
    }
  }

  it should "support groupBy()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4)).groupBy(_ % 2).mapValues(_.toSet)
      p.internal should containInAnyOrder (Seq((0, Set(2, 4)), (1, Set(1, 3))))
    }
  }

  it should "support keyBy()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("hello", "world")).keyBy(_.substring(0, 1))
      p.internal should containInAnyOrder (Seq(("h", "hello"), ("w", "world")))
    }
  }

  it should "support map()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq("1", "2", "3")).map(_.toInt)
      p.internal should containInAnyOrder (Seq(1, 2, 3))
    }
  }

  it should "support max()" in {
    runWithContext { sc =>
      def max[T: ClassTag : Numeric](elems: T*) = sc.parallelize(elems).max
      max(1, 2, 3).internal should containSingleValue (3)
      max(1L, 2L, 3L).internal should containSingleValue (3L)
      max(1F, 2F, 3F).internal should containSingleValue (3F)
      max(1.0, 2.0, 3.0).internal should containSingleValue (3.0)
    }
  }

  it should "support mean()" in {
    runWithContext { sc =>
      def mean[T: ClassTag : Numeric](elems: T*) = sc.parallelize(elems).mean
      mean(1, 2, 3).internal should containSingleValue (2.0)
      mean(1L, 2L, 3L).internal should containSingleValue (2.0)
      mean(1F, 2F, 3F).internal should containSingleValue (2.0)
      mean(1.0, 2.0, 3.0).internal should containSingleValue (2.0)
    }
  }

  it should "support min()" in {
    runWithContext { sc =>
      def min[T: ClassTag : Numeric](elems: T*) = sc.parallelize(elems).min
      min(1, 2, 3).internal should containSingleValue (1)
      min(1L, 2L, 3L).internal should containSingleValue (1L)
      min(1F, 2F, 3F).internal should containSingleValue (1F)
      min(1.0, 2.0, 3.0).internal should containSingleValue (1.0)
    }
  }

  it should "support quantilesApprox()" in {
    runWithContext { sc =>
      val p = sc.parallelize(0 to 100).quantilesApprox(5)
      p.internal should containSingleValue (iterable(0, 25, 50, 75, 100))
    }
  }

  it should "support randomSplit()" in {
    runWithContext { sc =>
      def round(c: Long): Long = math.round(c / 100.0) * 100
      val p1 = sc.parallelize(0 to 1000).randomSplit(Array(0.3, 0.7))
      val p2 = sc.parallelize(0 to 1000).randomSplit(Array(0.2, 0.3, 0.5))
      p1.length shouldBe 2
      p2.length shouldBe 3
      p1(0).count().map(round).internal should containSingleValue (300L)
      p1(1).count().map(round).internal should containSingleValue (700L)
      p2(0).count().map(round).internal should containSingleValue (200L)
      p2(1).count().map(round).internal should containSingleValue (300L)
      p2(2).count().map(round).internal should containSingleValue (500L)
    }
  }

  it should "support reduce()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5)).reduce(_ + _)
      p.internal should containSingleValue (15)
    }
  }

  it should "support sample()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 1, 1, 1, 1))
      val r1 = p.sample(1)
      val r2 = p.sample(5)
      r1.internal should containSingleValue (iterable(1))
      r2.internal should containSingleValue (iterable(1, 1, 1, 1, 1))
    }
  }

  it should "support sample() with replacement" in {
    runWithContext { sc =>
      import RandomSamplerUtils._
      verify(sc, true, 0.5, 0.5).internal should containSingleValue (true)
      verify(sc, true, 0.7, 0.7).internal should containSingleValue (true)
      verify(sc, true, 0.9, 0.9).internal should containSingleValue (true)
      verify(sc, true, 0.4, 0.6).internal should containSingleValue (false)
    }
  }

  it should "support sample() without replacement" in {
    runWithContext { sc =>
      import RandomSamplerUtils._
      verify(sc, false, 0.5, 0.5).internal should containSingleValue (true)
      verify(sc, false, 0.7, 0.7).internal should containSingleValue (true)
      verify(sc, false, 0.9, 0.9).internal should containSingleValue (true)
      verify(sc, false, 0.4, 0.6).internal should containSingleValue (false)
    }
  }

  it should "support subtract()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
      val p2 = sc.parallelize(Seq(2, 4, 6, 8, 10))
      val p = p1.subtract(p2)
      p.internal should containInAnyOrder (Seq(1, 3, 5))
    }
  }

  it should "support subtract() with duplicates" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 1, 3, 5))
      val p2 = sc.parallelize(Seq(2, 4, 6, 8, 10))
      val p = p1.subtract(p2)
      p.internal should containInAnyOrder (Seq(1, 3, 5))
    }
  }

  it should "support sum()" in {
    runWithContext { sc =>
      def sum[T: ClassTag : Semigroup](elems: T*) = sc.parallelize(elems).sum
      sum(1, 2, 3).internal should containSingleValue (6)
      sum(1L, 2L, 3L).internal should containSingleValue (6L)
      sum(1F, 2F, 3F).internal should containSingleValue (6F)
      sum(1.0, 2.0, 3.0).internal should containSingleValue (6.0)
      sum(1 to 100: _*).internal should containSingleValue (5050)
    }
  }

  it should "support take()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
      val r1 = p.take(1).count()
      val r2 = p.take(2).count()
      r1.internal should containSingleValue (1L)
      r2.internal should containSingleValue (2L)
    }
  }

  it should "support top()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
      val r1 = p.top(3)
      val r2 = p.top(3)(Ordering.by(-_))
      r1.internal should containSingleValue (iterable(5, 4, 3))
      r2.internal should containSingleValue (iterable(1, 2, 3))
    }
  }

  it should "support cross()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1))
      val p3 = sc.parallelize(Seq(1, 2))
      val s1 = p1.cross(p2)
      val s2 = p1.cross(p3)
      s1.internal should containInAnyOrder (Seq(("a", 1), ("b", 1), ("c", 1)))
      s2.internal should containInAnyOrder (Seq(("a", 1), ("a", 2), ("b", 1), ("b", 2), ("c", 1), ("c", 2)))
    }
  }

  it should "support hashLookup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(("a", 1), ("b", 2), ("b", 3)))
      val p = p1.hashLookup(p2).mapValues(_.toSet)
      p.internal should containInAnyOrder (Seq(("a", Set(1)), ("b", Set(2, 3)), ("c", Set[Int]())))
    }
  }

  it should "support withFixedWindows()" in {
    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(Seq("a", "b", "c", "d", "e", "f"), (0 to 5).map(new Instant(_)))
      val r = p.withFixedWindows(Duration.millis(3)).top(10).map(_.toSet)
      r.internal should containInAnyOrder (Seq(Set("a", "b", "c"), Set("d", "e", "f")))
    }
  }

  it should "support withSlidingWindows()" in {
    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(Seq("a", "b", "c", "d", "e", "f"), (0 to 5).map(new Instant(_)))
      val r = p.withSlidingWindows(Duration.millis(2), Duration.millis(3)).top(10).map(_.toSet)
      r.internal should containInAnyOrder (Seq(Set("a", "b"), Set("d", "e")))
    }
  }

  it should "support withSessionWindows()" in {
    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(Seq("a", "b", "c", "d", "e"), Seq(0, 5, 10, 44, 55).map(new Instant(_)))
      val r = p
        .withSessionWindows(Duration.millis(10)).top(10).map(_.toSet)
      r.internal should containInAnyOrder (Seq(Set("a", "b", "c"), Set("d"), Set("e")))
    }
  }

  it should "support withGlobalWindow()" in {
    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(Seq("a", "b", "c", "d", "e", "f"), (0 to 5).map(new Instant(_)))
      val r = p.withFixedWindows(Duration.millis(3)).withGlobalWindow().top(10).map(_.toSet)
      r.internal should containInAnyOrder (Seq(Set("a", "b", "c", "d", "e", "f")))
    }
  }

  it should "support withTimestamp()"  in {
    runWithContext { sc =>
      val p = sc.parallelizeTimestamped(Seq("a", "b", "c"), Seq(1, 2, 3).map(new Instant(_)))
      val r = p.withTimestamp().map(kv => (kv._1, kv._2.getMillis))
      r.internal should containInAnyOrder (Seq(("a", 1L), ("b", 2L), ("c", 3L)))
    }
  }

  it should "support timestampBy()"  in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3))
      val r = p.timestampBy(new Instant(_)).withTimestamp().map(kv => (kv._1, kv._2.getMillis))
      r.internal should containInAnyOrder (Seq((1, 1L), (2, 2L), (3, 3L)))
    }
  }

}
