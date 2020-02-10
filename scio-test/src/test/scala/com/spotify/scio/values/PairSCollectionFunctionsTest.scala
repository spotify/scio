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

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.random.RandomSamplerUtils
import com.twitter.algebird.Aggregator
import magnolify.guava.auto._

class PairSCollectionFunctionsTest extends PipelineSpec {
  "PairSCollection" should "support cogroup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11L), ("b", 12L), ("d", 14L)))
      val r1 = p1.cogroup(p2)
      val r2 = p1.groupWith(p2)
      val expected = Seq(
        ("a", (Iterable(1), Iterable(11L))),
        ("b", (Iterable(2), Iterable(12L))),
        ("c", (Iterable(3), Iterable())),
        ("d", (Iterable(), Iterable(14L)))
      )
      r1 should containInAnyOrder(expected)
      r2 should containInAnyOrder(expected)
    }
  }

  it should "support cogroup() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 2), ("c", 3)))
      val p2 =
        sc.parallelize(Seq(("a", 11L), ("b", 12L), ("b", 13L), ("d", 14L)))
      val fn =
        (t: (String, (Iterable[Int], Iterable[Long]))) => (t._1, (t._2._1.toSet, t._2._2.toSet))
      val r1 = p1.cogroup(p2).map(fn)
      val r2 = p1.groupWith(p2).map(fn)
      val expected =
        Seq[(String, (Set[Int], Set[Long]))](
          ("a", (Set(1, 2), Set(11L))),
          ("b", (Set(2), Set(12L, 13L))),
          ("c", (Set(3), Set())),
          ("d", (Set(), Set(14L)))
        )
      r1 should containInAnyOrder(expected)
      r2 should containInAnyOrder(expected)
    }
  }

  it should "support 3-way cogroup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11L), ("b", 12L), ("d", 14L)))
      val p3 = sc.parallelize(Seq(("a", 21f), ("b", 22f), ("e", 25f)))
      val r1 = p1.cogroup(p2, p3)
      val r2 = p1.groupWith(p2, p3)
      val expected = Seq(
        ("a", (Iterable(1), Iterable(11L), Iterable(21f))),
        ("b", (Iterable(2), Iterable(12L), Iterable(22f))),
        ("c", (Iterable(3), Iterable(), Iterable())),
        ("d", (Iterable(), Iterable(14L), Iterable())),
        ("e", (Iterable(), Iterable(), Iterable(25f)))
      )
      r1 should containInAnyOrder(expected)
      r2 should containInAnyOrder(expected)
    }
  }

  it should "support 4-way cogroup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11L), ("b", 12L), ("d", 14L)))
      val p3 = sc.parallelize(Seq(("a", 21f), ("b", 22f), ("e", 25f)))
      val p4 = sc.parallelize(Seq(("a", 31.0), ("b", 32.0), ("f", 36.0)))
      val r1 = p1.cogroup(p2, p3, p4)
      val r2 = p1.groupWith(p2, p3, p4)
      val expected = Seq(
        ("a", (Iterable(1), Iterable(11L), Iterable(21f), Iterable(31.0))),
        ("b", (Iterable(2), Iterable(12L), Iterable(22f), Iterable(32.0))),
        ("c", (Iterable(3), Iterable(), Iterable(), Iterable())),
        ("d", (Iterable(), Iterable(14L), Iterable(), Iterable())),
        ("e", (Iterable(), Iterable(), Iterable(25f), Iterable())),
        ("f", (Iterable(), Iterable(), Iterable(), Iterable(36.0)))
      )
      r1 should containInAnyOrder(expected)
      r2 should containInAnyOrder(expected)
    }
  }

  it should "support fullOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.fullOuterJoin(p2)
      p should containInAnyOrder(
        Seq(
          ("a", (Some(1), Some(11))),
          ("b", (Some(2), Some(12))),
          ("c", (Some(3), None)),
          ("d", (None, Some(14)))
        )
      )
    }
  }

  it should "support fullOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = p1.fullOuterJoin(p2)
      p should containInAnyOrder(
        Seq(
          ("a", (Some(1), Some(11))),
          ("a", (Some(2), Some(11))),
          ("b", (Some(3), Some(12))),
          ("b", (Some(3), Some(13))),
          ("c", (Some(4), None)),
          ("d", (None, Some(14)))
        )
      )
    }
  }

  it should "support join()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.join(p2)
      p should containInAnyOrder(Seq(("a", (1, 11)), ("b", (2, 12))))
    }
  }

  it should "support leftOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.leftOuterJoin(p2)
      p should containInAnyOrder(Seq(("a", (1, Some(11))), ("b", (2, Some(12))), ("c", (3, None))))
    }
  }

  it should "support leftOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = p1.leftOuterJoin(p2)
      p should containInAnyOrder(
        Seq(
          ("a", (1, Some(11))),
          ("a", (2, Some(11))),
          ("b", (3, Some(12))),
          ("b", (3, Some(13))),
          ("c", (4, None))
        )
      )
    }
  }

  it should "support rightOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.rightOuterJoin(p2)
      p should
        containInAnyOrder(Seq(("a", (Some(1), 11)), ("b", (Some(2), 12)), ("d", (None, 14))))
    }
  }

  it should "support rightOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = p1.rightOuterJoin(p2)
      p should containInAnyOrder(
        Seq(
          ("a", (Some(1), 11)),
          ("a", (Some(2), 11)),
          ("b", (Some(3), 12)),
          ("b", (Some(3), 13)),
          ("d", (None, 14))
        )
      )
    }
  }

  it should "support aggregateByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 100).map(("a", _))
      val p2 = sc.parallelize(1 to 10).map(("b", _))
      val r1 = (p1 ++ p2).aggregateByKey(0.0)(_ + _, _ + _)
      val r2 = (p1 ++ p2).aggregateByKey(Aggregator.max[Int])
      val r3 =
        (p1 ++ p2).aggregateByKey(Aggregator.immutableSortedReverseTake[Int](5))
      r1 should containInAnyOrder(Seq(("a", 5050.0), ("b", 55.0)))
      r2 should containInAnyOrder(Seq(("a", 100), ("b", 10)))
      r3 should containInAnyOrder(Seq(("a", Seq(100, 99, 98, 97, 96)), ("b", Seq(10, 9, 8, 7, 6))))
    }
  }

  it should "support approxQuantilesByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(0 to 100).map(("a", _))
      val p2 = sc.parallelize(0 to 10).map(("b", _))
      val p = (p1 ++ p2).approxQuantilesByKey(3)
      p should containInAnyOrder(Seq(("a", Iterable(0, 50, 100)), ("b", Iterable(0, 5, 10))))
    }
  }

  it should "support combineByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 100).map(("a", _))
      val p2 = sc.parallelize(1 to 10).map(("b", _))
      val p = (p1 ++ p2).combineByKey(_.toDouble)(_ + _)(_ + _)
      p should containInAnyOrder(Seq(("a", 5050.0), ("b", 55.0)))
    }
  }

  it should "support countApproxDistinctByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 11), ("a", 12), ("b", 21), ("b", 22), ("b", 23)))
      val r1 = p.countApproxDistinctByKey()
      val r2 = p.countApproxDistinctByKey(sampleSize = 10000)
      r1 should containInAnyOrder(Seq(("a", 2L), ("b", 3L)))
      r2 should containInAnyOrder(Seq(("a", 2L), ("b", 3L)))
    }
  }

  it should "support countByKey()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 11), ("a", 12), ("b", 21), ("b", 22), ("b", 23)))
        .countByKey
      p should containInAnyOrder(Seq(("a", 2L), ("b", 3L)))
    }
  }

  it should "support distinctByKey() on String keys" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 11), ("a", 11), ("b", 22), ("b", 22), ("b", 22)))
        .distinctByKey
      p should containInAnyOrder(Seq(("a", 11), ("b", 22)))
    }
  }

  it should "support distinctByKey() on Scala Long keys" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq((1L, 11), (1L, 11), (2L, 22), (2L, 22), (2L, 22)))
        .distinctByKey
      p should containInAnyOrder(Seq((1L, 11), (2L, 22)))
    }
  }

  it should "support distinctByKey() on Scala Int keys" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq((1, 11), (1, 11), (2, 22), (2, 22), (2, 22)))
        .distinctByKey
      p should containInAnyOrder(Seq((1, 11), (2, 22)))
    }
  }

  it should "support filterValues()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)))
        .filterValues(_ % 2 == 0)
      p should containInAnyOrder(Seq(("b", 2), ("d", 4)))
    }
  }

  it should "support flatMapValues()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 1), ("b", 2)))
        .flatMapValues(v => Seq(v + 10.0, v + 20.0))
      p should containInAnyOrder(Seq(("a", 11.0), ("a", 21.0), ("b", 12.0), ("b", 22.0)))
    }
  }

  it should "support foldByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 100).map(("a", _))
      val p2 = sc.parallelize(1 to 10).map(("b", _))
      val r1 = (p1 ++ p2).foldByKey(0)(_ + _)
      val r2 = (p1 ++ p2).foldByKey
      r1 should containInAnyOrder(Seq(("a", 5050), ("b", 55)))
      r2 should containInAnyOrder(Seq(("a", 5050), ("b", 55)))
    }
  }

  it should "support groupByKey" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 1), ("a", 10), ("b", 2), ("b", 20)))
        .groupByKey
        .mapValues(_.toSet)
      p should containInAnyOrder(Seq(("a", Set(1, 10)), ("b", Set(2, 20))))
    }
  }

  it should "support batchByKey" in {
    runWithContext { sc =>
      val batchSize = 2
      val nonEmpty = sc
        .parallelize(Seq(("a", 1), ("a", 10), ("b", 2), ("b", 20)))
        .batchByKey(batchSize)
        .mapValues(_.toSet)
      nonEmpty should containInAnyOrder(Seq(("a", Set(1, 10)), ("b", Set(2, 20))))

      val empty = sc.empty[(String, Int)]().batchByKey(batchSize)
      empty should beEmpty
    }
  }

  it should "support intersectByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.intersectByKey(p2)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2)))
    }
  }

  it should "support intersectByKey() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq("a", "b", "b", "d"))
      val p = p1.intersectByKey(p2)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2), ("b", 4)))
    }
  }

  it should "support intersectByKey() with empty LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq[(String, Unit)]())
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.intersectByKey(p2)
      p should beEmpty
    }
  }

  it should "support intersectByKey() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq[String]())
      val p = p1.intersectByKey(p2)
      p should beEmpty
    }
  }

  it should "support sparseIntersectByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.sparseIntersectByKey(p2, 5)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2)))
    }
  }

  it should "support sparseIntersectByKey() with computeExact set to true" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.sparseIntersectByKey(p2, 5, computeExact = true)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2)))
    }
  }

  it should "support sparseIntersectByKey() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq("a", "b", "b", "d"))
      val p = p1.sparseIntersectByKey(p2, 5)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2), ("b", 4)))
    }
  }

  it should "support sparseIntersectByKey() with partitions" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.sparseIntersectByKey(p2, 1000000000)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2)))
    }
  }

  it should "support sparseIntersectByKey() with empty LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq[(String, Unit)]())
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.sparseIntersectByKey(p2, 5)
      p should beEmpty
    }
  }

  it should "support sparseIntersectByKey() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq[String]())
      val p = p1.sparseIntersectByKey(p2, 5)
      p should beEmpty
    }
  }

  it should "support keys()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).keys
      p should containInAnyOrder(Seq("a", "b", "c"))
    }
  }

  it should "support mapValues()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2))).mapValues(_ + 10.0)
      p should containInAnyOrder(Seq(("a", 11.0), ("b", 12.0)))
    }
  }

  it should "support maxByKey()" in {
    runWithContext { sc =>
      val p =
        sc.parallelize(Seq(("a", 1), ("a", 10), ("b", 2), ("b", 20))).maxByKey
      p should containInAnyOrder(Seq(("a", 10), ("b", 20)))
    }
  }

  it should "support minByKey()" in {
    runWithContext { sc =>
      val p =
        sc.parallelize(Seq(("a", 1), ("a", 10), ("b", 2), ("b", 20))).minByKey
      p should containInAnyOrder(Seq(("a", 1), ("b", 2)))
    }
  }

  it should "support reduceByKey()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 1), ("b", 1), ("b", 2), ("c", 1), ("c", 2), ("c", 3)))
        .reduceByKey(_ + _)
      p should containInAnyOrder(Seq(("a", 1), ("b", 3), ("c", 6)))
    }
  }

  it should "support sampleByKey()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 1), ("b", 2), ("b", 2), ("c", 3), ("c", 3), ("c", 3)))
        .sampleByKey(1)
      p should containInAnyOrder(Seq(("a", Iterable(1)), ("b", Iterable(2)), ("c", Iterable(3))))
    }
  }

  it should "support sampleByKey() with replacement" in {
    import RandomSamplerUtils._
    for (fraction <- List(0.05, 0.2, 1.0)) {
      val sample = runWithData(keyedPopulation) {
        _.sampleByKey(true, Map("a" -> fraction, "b" -> fraction))
      }
      sample.groupBy(_._1).values.foreach { s =>
        (s.size.toDouble / populationSize) shouldBe fraction +- 0.05
        s.toSet.size should be < sample.size
      }
    }
  }

  it should "support sampleByKey() without replacement" in {
    import RandomSamplerUtils._
    for (fraction <- List(0.05, 0.2, 1.0)) {
      val sample = runWithData(keyedPopulation) {
        _.sampleByKey(false, Map("a" -> fraction, "b" -> fraction))
      }
      sample.groupBy(_._1).values.foreach { s =>
        (s.size.toDouble / populationSize) shouldBe fraction +- 0.05
        s.toSet.size should be < sample.size
      }
    }
  }

  it should "support subtractByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("b", 3), ("c", 4), ("c", 5), ("c", 6)))
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.subtractByKey(p2)
      p should containInAnyOrder(Seq(("c", 4), ("c", 5), ("c", 6)))
    }
  }

  it should "support subtractByKey() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("b", 3), ("c", 4), ("c", 5), ("c", 6)))
      val p2 = sc.parallelize(Seq("a", "b", "b", "d"))
      val p = p1.subtractByKey(p2)
      p should containInAnyOrder(Seq(("c", 4), ("c", 5), ("c", 6)))
    }
  }

  it should "support subtractByKey() with empty LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq[(String, Unit)]())
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.subtractByKey(p2)
      p should beEmpty
    }
  }

  it should "support subtractByKey() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq[String]())
      val p = p1.subtractByKey(p2)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
    }
  }

  it should "support sumByKey()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(List(("a", 1), ("b", 2), ("b", 2)) ++ (1 to 100).map(("c", _)))
        .sumByKey
      p should containInAnyOrder(Seq(("a", 1), ("b", 4), ("c", 5050)))
    }
  }

  it should "support swap()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).swap
      p should containInAnyOrder(Seq((1, "a"), (2, "b"), (3, "c")))
    }
  }

  it should "support topByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 11), ("b", 12), ("c", 21), ("c", 22), ("c", 23)))
      val r1 = p.topByKey(1)
      val r2 = p.topByKey(1, Ordering.by(-_))
      r1 should
        containInAnyOrder(Seq(("a", Iterable(1)), ("b", Iterable(12)), ("c", Iterable(23))))
      r2 should
        containInAnyOrder(Seq(("a", Iterable(1)), ("b", Iterable(11)), ("c", Iterable(21))))
    }
  }

  it should "support values()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).values
      p should containInAnyOrder(Seq(1, 2, 3))
    }
  }

  it should "support flattenValues()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", Seq(1, 2, 3)), ("b", Seq(4, 5, 6))))
        .flattenValues[Int]
      p should containInAnyOrder(Seq(("a", 1), ("a", 2), ("a", 3), ("b", 4), ("b", 5), ("b", 6)))
    }
  }

  it should "support hashPartitionByKey()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq((1, Seq(1, 2, 3)), (-2, Seq(4, 5, 6))))
        .hashPartitionByKey(2)
      p(0) should containInAnyOrder(Seq((-2, Seq(4, 5, 6))))
      p(1) should containInAnyOrder(Seq((1, Seq(1, 2, 3))))
    }
  }

  val sparseLhs = Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4))
  val sparseRhs = Seq(("a", 11), ("d", 5))
  val sparseFullOuterJoinExpected = Seq(
    ("a", (Some(1), Some(11))),
    ("a", (Some(2), Some(11))),
    ("b", (Some(3), None)),
    ("c", (Some(4), None)),
    ("d", (None, Some(5)))
  )

  val sparseJoinExpected =
    Seq(("a", (1, 11)), ("a", (2, 11)))

  val sparseRightOuterJoinExpected =
    Seq(("a", (Some(1), 11)), ("a", (Some(2), 11)), ("d", (None, 5)))
  val sparseLeftOuterJoinExpected =
    Seq(("a", (1, Some(11))), ("a", (2, Some(11))), ("b", (3, None)), ("c", (4, None)))

  it should "support sparseFullOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseRhs)
      val p = p1.sparseFullOuterJoin(p2, 10)
      val pd = p1.sparseOuterJoin(p2, 10) // Test deprecated method
      p should containInAnyOrder(sparseFullOuterJoinExpected)
      pd should containInAnyOrder(sparseFullOuterJoinExpected)
    }
  }

  it should "support sparseFullOuterJoin() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(Seq[(String, Int)]())
      val p = p1.sparseFullOuterJoin(p2, 10)
      val pd = p1.sparseOuterJoin(p2, 10) // Test deprecated method
      p should containInAnyOrder(
        Seq[(String, (Option[Int], Option[Int]))](
          ("a", (Some(1), None)),
          ("a", (Some(2), None)),
          ("b", (Some(3), None)),
          ("c", (Some(4), None))
        )
      )
      pd should containInAnyOrder(
        Seq[(String, (Option[Int], Option[Int]))](
          ("a", (Some(1), None)),
          ("a", (Some(2), None)),
          ("b", (Some(3), None)),
          ("c", (Some(4), None))
        )
      )
    }
  }

  it should "support sparseFullOuterJoin() with partitions" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseRhs)
      val p = p1.sparseFullOuterJoin(p2, 1000000000L)
      val pd = p1.sparseOuterJoin(p2, 10) // Test deprecated method
      p should containInAnyOrder(sparseFullOuterJoinExpected)
      pd should containInAnyOrder(sparseFullOuterJoinExpected)
    }
  }

  it should "support sparseJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseRhs)
      val p = p1.sparseJoin(p2, 10)
      p should containInAnyOrder(sparseJoinExpected)
    }
  }

  it should "support sparseJoin() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(Seq[(String, Int)]())
      val p = p1.sparseJoin(p2, 10)
      p should containInAnyOrder(Seq.empty[(String, (Int, Int))])
    }
  }

  it should "support sparseJoin() with partitions" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseRhs)
      val p = p1.sparseJoin(p2, 1000000000L)
      p should containInAnyOrder(sparseJoinExpected)
    }
  }

  it should "support sparseLeftOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseRhs)
      val p = p1.sparseLeftOuterJoin(p2, 10)
      p should containInAnyOrder(sparseLeftOuterJoinExpected)
    }
  }

  it should "support sparseLeftOuterJoin() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(Seq[(String, Int)]())
      val p = p1.sparseLeftOuterJoin(p2, 10)
      p should containInAnyOrder(
        Seq[(String, (Int, Option[Int]))](
          ("a", (1, None)),
          ("a", (2, None)),
          ("b", (3, None)),
          ("c", (4, None))
        )
      )
    }
  }

  it should "support sparseLeftOuterJoin() with partitions" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseRhs)
      val p = p1.sparseLeftOuterJoin(p2, 1000000000L)
      p should containInAnyOrder(sparseLeftOuterJoinExpected)
    }
  }

  it should "support sparseRightOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseRhs)
      val p = p1.sparseRightOuterJoin(p2, 10)
      p should containInAnyOrder(sparseRightOuterJoinExpected)
    }
  }

  it should "support sparseRightOuterJoin() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(Seq[(String, Int)]())
      val p = p1.sparseRightOuterJoin(p2, 10)
      p should beEmpty
    }
  }

  it should "support sparseRightOuterJoin() with partitions" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseRhs)
      val p = p1.sparseRightOuterJoin(p2, 1000000000L)
      p should containInAnyOrder(sparseRightOuterJoinExpected)
    }
  }

  val sparseLookup1 = Seq(("a", 11), ("a", 12), ("b", 13), ("d", 15), ("e", 16))
  val sparseLookup2 = Seq(("a", 21), ("a", 22), ("b", 23), ("d", 25), ("e", 26))
  val expected1: Seq[(String, (Int, Set[Int]))] =
    Seq(("a", (1, Set(11, 12))), ("a", (2, Set(11, 12))), ("b", (3, Set(13))), ("c", (4, Set())))
  val expected12: Seq[(String, (Int, Set[Int], Set[Int]))] =
    Seq(
      ("a", (1, Set(11, 12), Set(21, 22))),
      ("a", (2, Set(11, 12), Set(21, 22))),
      ("b", (3, Set(13), Set(23))),
      ("c", (4, Set(), Set()))
    )

  it should "support sparseLookup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseLookup1)
      val p = p1
        .sparseLookup(p2, 10)
        .mapValues(v => (v._1, v._2.toSet))

      p should containInAnyOrder(expected1)
    }
  }

  it should "support sparseLookup() with partitions" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseLookup1)
      val p = p1
        .sparseLookup(p2, 1000000000L)
        .mapValues(v => (v._1, v._2.toSet))

      p should containInAnyOrder(expected1)
    }
  }

  it should "support sparseLookup2()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseLookup1)
      val p3 = sc.parallelize(sparseLookup2)
      val p = p1
        .sparseLookup(p2, p3, 10)
        .mapValues(v => (v._1, v._2.toSet, v._3.toSet))

      p should containInAnyOrder(expected12)
    }
  }

  it should "support sparseLookup2() with partitions" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(sparseLhs)
      val p2 = sc.parallelize(sparseLookup1)
      val p3 = sc.parallelize(sparseLookup2)
      val p = p1
        .sparseLookup(p2, p3, 1000000000L)
        .mapValues(v => (v._1, v._2.toSet, v._3.toSet))

      p should containInAnyOrder(expected12)
    }
  }

  it should "support sparseLookup() with Empty LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq[(String, Unit)]())
      val p2 = sc.parallelize(sparseLookup1)
      val p = p1.sparseLookup(p2, 10)

      p should beEmpty
    }
  }

  it should "support sparseLookup2() with Empty LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq[(String, Unit)]())
      val p2 = sc.parallelize(sparseLookup1)
      val p3 = sc.parallelize(sparseLookup2)
      val p = p1.sparseLookup(p2, p3, 10)

      p should beEmpty
    }
  }

  it should "support join() of empty SCollections" in {
    runWithContext { sc =>
      val lhs = sc.parallelize(Seq[(String, Unit)]())
      val rhs = sc.parallelize(Seq[(String, Unit)]())
      val result = lhs.join(rhs)
      result should beEmpty
    }
  }

  it should "support negative hashCodes in sparse implementations" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq((-1, 11), (2, 12)))
      val p2 = sc.parallelize(Seq((-1, 111), (2, 122), (-3, 133), (6, 166)))
      val p = p1.sparseRightOuterJoin(p2, 1000000000L)
      val expected = Seq(
        (-1, (Some(11), 111)),
        (2, (Some(12), 122)),
        (-3, (None, 133)),
        (6, (None, 166))
      )
      p should containInAnyOrder(expected)
    }
  }
}
