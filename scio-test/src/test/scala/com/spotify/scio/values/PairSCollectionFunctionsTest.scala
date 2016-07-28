/*
 * Copyright 2016 Spotify AB.
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

class PairSCollectionFunctionsTest extends PipelineSpec {

  import com.spotify.scio.testing.TestingUtils._

  "PairSCollection" should "support cogroup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11L), ("b", 12L), ("d", 14L)))
      val r1 = p1.cogroup(p2)
      val r2 = p1.groupWith(p2)
      val expected = Seq(
        ("a", (iterable(1), iterable(11L))),
        ("b", (iterable(2), iterable(12L))),
        ("c", (iterable(3), iterable())),
        ("d", (iterable(), iterable(14L))))
      r1 should containInAnyOrder (expected)
      r2 should containInAnyOrder (expected)
    }
  }

  it should "support cogroup() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11L), ("b", 12L), ("b", 13L), ("d", 14L)))
      val fn = (t: (String, (Iterable[Int], Iterable[Long]))) =>
        (t._1, (t._2._1.toSet, t._2._2.toSet))
      val r1 = p1.cogroup(p2).map(fn)
      val r2 = p1.groupWith(p2).map(fn)
      val expected = Seq[(String, (Set[Int], Set[Long]))](
        ("a", (Set(1, 2), Set(11L))),
        ("b", (Set(2), Set(12L, 13L))),
        ("c", (Set(3), Set())),
        ("d", (Set(), Set(14L))))
      r1 should containInAnyOrder (expected)
      r2 should containInAnyOrder (expected)
    }
  }

  it should "support 3-way cogroup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11L), ("b", 12L), ("d", 14L)))
      val p3 = sc.parallelize(Seq(("a", 21F), ("b", 22F), ("e", 25F)))
      val r1 = p1.cogroup(p2, p3)
      val r2 = p1.groupWith(p2, p3)
      val expected = Seq(
        ("a", (iterable(1), iterable(11L), iterable(21F))),
        ("b", (iterable(2), iterable(12L), iterable(22F))),
        ("c", (iterable(3), iterable(), iterable())),
        ("d", (iterable(), iterable(14L), iterable())),
        ("e", (iterable(), iterable(), iterable(25F))))
      r1 should containInAnyOrder (expected)
      r2 should containInAnyOrder (expected)
    }
  }

  it should "support 4-way cogroup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11L), ("b", 12L), ("d", 14L)))
      val p3 = sc.parallelize(Seq(("a", 21F), ("b", 22F), ("e", 25F)))
      val p4 = sc.parallelize(Seq(("a", 31.0), ("b", 32.0), ("f", 36.0)))
      val r1 = p1.cogroup(p2, p3, p4)
      val r2 = p1.groupWith(p2, p3, p4)
      val expected = Seq(
        ("a", (iterable(1), iterable(11L), iterable(21F), iterable(31.0))),
        ("b", (iterable(2), iterable(12L), iterable(22F), iterable(32.0))),
        ("c", (iterable(3), iterable(), iterable(), iterable())),
        ("d", (iterable(), iterable(14L), iterable(), iterable())),
        ("e", (iterable(), iterable(), iterable(25F), iterable())),
        ("f", (iterable(), iterable(), iterable(), iterable(36.0))))
      r1 should containInAnyOrder (expected)
      r2 should containInAnyOrder (expected)
    }
  }

  it should "support fullOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.fullOuterJoin(p2)
      p should containInAnyOrder (Seq(
        ("a", (Some(1), Some(11))),
        ("b", (Some(2), Some(12))),
        ("c", (Some(3), None)),
        ("d", (None, Some(14)))))
    }
  }

  it should "support fullOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = p1.fullOuterJoin(p2)
      p should containInAnyOrder (Seq(
        ("a", (Some(1), Some(11))),
        ("a", (Some(2), Some(11))),
        ("b", (Some(3), Some(12))),
        ("b", (Some(3), Some(13))),
        ("c", (Some(4), None)),
        ("d", (None, Some(14)))))
    }
  }

  it should "support join()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.join(p2)
      p should containInAnyOrder (Seq(("a", (1, 11)), ("b", (2, 12))))
    }
  }

  it should "support leftOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.leftOuterJoin(p2)
      p should containInAnyOrder (Seq(("a", (1, Some(11))), ("b", (2, Some(12))), ("c", (3, None))))
    }
  }

  it should "support leftOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = p1.leftOuterJoin(p2)
      p should containInAnyOrder (Seq(
        ("a", (1, Some(11))),
        ("a", (2, Some(11))),
        ("b", (3, Some(12))),
        ("b", (3, Some(13))),
        ("c", (4, None))))
    }
  }

  it should "support rightOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.rightOuterJoin(p2)
      p should
        containInAnyOrder (Seq(("a", (Some(1), 11)), ("b", (Some(2), 12)), ("d", (None, 14))))
    }
  }

  it should "support rightOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = p1.rightOuterJoin(p2)
      p should containInAnyOrder (Seq(
        ("a", (Some(1), 11)),
        ("a", (Some(2), 11)),
        ("b", (Some(3), 12)),
        ("b", (Some(3), 13)),
        ("d", (None, 14))))
    }
  }

  it should "support aggregateByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 100).map(("a", _))
      val p2 = sc.parallelize(1 to 10).map(("b", _))
      val r1 = (p1 ++ p2).aggregateByKey(0.0)(_ + _, _ + _)
      val r2 = (p1 ++ p2).aggregateByKey(Aggregator.max[Int])
      val r3 = (p1 ++ p2).aggregateByKey(Aggregator.immutableSortedReverseTake[Int](5))
      r1 should containInAnyOrder (Seq(("a", 5050.0), ("b", 55.0)))
      r2 should containInAnyOrder (Seq(("a", 100), ("b", 10)))
      r3 should containInAnyOrder (Seq(("a", Seq(100, 99, 98, 97, 96)), ("b", Seq(10, 9, 8, 7, 6))))
    }
  }

  it should "support approxQuantilesByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(0 to 100).map(("a", _))
      val p2 = sc.parallelize(0 to 10).map(("b", _))
      val p = (p1 ++ p2).approxQuantilesByKey(3)
      p should containInAnyOrder (Seq(("a", iterable(0, 50, 100)), ("b", iterable(0, 5, 10))))
    }
  }

  it should "support combineByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 100).map(("a", _))
      val p2 = sc.parallelize(1 to 10).map(("b", _))
      val p = (p1 ++ p2).combineByKey(_.toDouble)(_ + _)(_ + _)
      p should containInAnyOrder (Seq(("a", 5050.0), ("b", 55.0)))
    }
  }

  it should "support countApproxDistinctByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 11), ("a", 12), ("b", 21), ("b", 22), ("b", 23)))
      val r1 = p.countApproxDistinctByKey()
      val r2 = p.countApproxDistinctByKey(sampleSize = 10000)
      r1 should containInAnyOrder (Seq(("a", 2L), ("b", 3L)))
      r2 should containInAnyOrder (Seq(("a", 2L), ("b", 3L)))
    }
  }

  it should "support countByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 11), ("a", 12), ("b", 21), ("b", 22), ("b", 23))).countByKey
      p should containInAnyOrder (Seq(("a", 2L), ("b", 3L)))
    }
  }

  it should "support flatMapValues()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2))).flatMapValues(v => Seq(v + 10.0, v + 20.0))
      p should containInAnyOrder (Seq(("a", 11.0), ("a", 21.0), ("b", 12.0), ("b", 22.0)))
    }
  }

  it should "support foldByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 100).map(("a", _))
      val p2 = sc.parallelize(1 to 10).map(("b", _))
      val r1 = (p1 ++ p2).foldByKey(0)(_ + _)
      val r2 = (p1 ++ p2).foldByKey
      r1 should containInAnyOrder (Seq(("a", 5050), ("b", 55)))
      r2 should containInAnyOrder (Seq(("a", 5050), ("b", 55)))
    }
  }

  it should "support groupByKey" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 1), ("a", 10), ("b", 2), ("b", 20)))
        .groupByKey
        .mapValues(_.toSet)
      p should containInAnyOrder (Seq(("a", Set(1, 10)), ("b", Set(2, 20))))
    }
  }

  it should "support intersectByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.intersectByKey(p2)
      p should containInAnyOrder (Seq(("a", 1), ("b", 2)))
    }
  }

  it should "support intersectByKey() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq("a", "b", "b", "d"))
      val p = p1.intersectByKey(p2)
      p should containInAnyOrder (Seq(("a", 1), ("b", 2), ("b", 4)))
    }
  }

  it should "support intersectByKey() with empty LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq[(String, Any)]())
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

  it should "support keys()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).keys
      p should containInAnyOrder (Seq("a", "b", "c"))
    }
  }

  it should "support mapValues()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2))).mapValues(_ + 10.0)
      p should containInAnyOrder (Seq(("a", 11.0), ("b", 12.0)))
    }
  }

  it should "support maxByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("a", 10), ("b", 2), ("b", 20))).maxByKey
      p should containInAnyOrder (Seq(("a", 10), ("b", 20)))
    }
  }

  it should "support minByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("a", 10), ("b", 2), ("b", 20))).minByKey
      p should containInAnyOrder (Seq(("a", 1), ("b", 2)))
    }
  }

  it should "support reduceByKey()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 1), ("b", 1), ("b", 2), ("c", 1), ("c", 2), ("c", 3)))
        .reduceByKey(_ + _)
      p should containInAnyOrder (Seq(("a", 1), ("b", 3), ("c", 6)))
    }
  }

  it should "support sampleByKey()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq(("a", 1), ("b", 2), ("b", 2), ("c", 3), ("c", 3), ("c", 3)))
        .sampleByKey(1)
      p should containInAnyOrder (Seq(("a", iterable(1)), ("b", iterable(2)), ("c", iterable(3))))
    }
  }

  it should "support sampleByKey() with replacement" in {
    import RandomSamplerUtils._
    for (fraction <- List(0.05, 0.2, 1.0)) {
      val sample = runWithData(keyedPopulation) {
        _.sampleByKey(true, Map("a" -> fraction, "b" -> fraction))
      }
      sample.groupBy(_._1).values.foreach { s =>
        (s.size.toDouble / populationSize) should be (fraction +- 0.05)
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
        (s.size.toDouble / populationSize) should be (fraction +- 0.05)
        s.toSet.size should be < sample.size
      }
    }
  }

  it should "support subtractByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("b", 3), ("c", 4), ("c", 5), ("c", 6)))
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.subtractByKey(p2)
      p should containInAnyOrder (Seq(("c", 4), ("c", 5), ("c", 6)))
    }
  }

  it should "support subtractByKey() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("b", 3), ("c", 4), ("c", 5), ("c", 6)))
      val p2 = sc.parallelize(Seq("a", "b", "b", "d"))
      val p = p1.subtractByKey(p2)
      p should containInAnyOrder (Seq(("c", 4), ("c", 5), ("c", 6)))
    }
  }

  it should "support subtrackByKey() with empty LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq[(String, Any)]())
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.subtractByKey(p2)
      p should beEmpty
    }
  }

  it should "support subtrackByKey() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq[String]())
      val p = p1.subtractByKey(p2)
      p should containInAnyOrder (Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
    }
  }

  it should "support sumByKey()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(List(("a", 1), ("b", 2), ("b", 2)) ++ (1 to 100).map(("c", _)))
        .sumByKey
      p should containInAnyOrder (Seq(("a", 1), ("b", 4), ("c", 5050)))
    }
  }

  it should "support swap()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).swap
      p should containInAnyOrder (Seq((1, "a"), (2, "b"), (3, "c")))
    }
  }

  it should "support topByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 11), ("b", 12), ("c", 21), ("c", 22), ("c", 23)))
      val r1 = p.topByKey(1)
      val r2 = p.topByKey(1)(Ordering.by(-_))
      r1 should
        containInAnyOrder (Seq(("a", iterable(1)), ("b", iterable(12)), ("c", iterable(23))))
      r2 should
        containInAnyOrder (Seq(("a", iterable(1)), ("b", iterable(11)), ("c", iterable(21))))
    }
  }

  it should "support values()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).values
      p should containInAnyOrder (Seq(1, 2, 3))
    }
  }

  it should "support hashJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.hashJoin(p2)
      p should containInAnyOrder (Seq(("a", (1, 11)), ("b", (2, 12))))
    }
  }

  it should "support hashJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.hashJoin(p2)
      p should
        containInAnyOrder (Seq(("a", (1, 11)), ("a", (2, 11)), ("b", (3, 12)), ("b", (3, 13))))
    }
  }

  it should "support hashLeftJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.hashLeftJoin(p2)
      p should containInAnyOrder (Seq(("a", (1, Some(11))), ("b", (2, Some(12))), ("c", (3, None))))
    }
  }

  it should "support hashLeftJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.hashLeftJoin(p2)
      p should containInAnyOrder (Seq(
        ("a", (1, Some(11))),
        ("a", (2, Some(11))),
        ("b", (3, Some(12))),
        ("b", (3, Some(13))),
        ("c", (4, None))))
    }
  }

  val (skewSeed, skewEps) = (42, 0.001D)

  it should "support skewedJoin() without hotkeys and no duplicate keys" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.skewedJoin(p2, Long.MaxValue, skewEps, skewSeed)
      p should
        containInAnyOrder (Seq(("a", (1, 11)), ("b", (2, 12)), ("b", (2, 13))))
    }
  }

  it should "support skewedJoin() without hotkeys" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.skewedJoin(p2, Long.MaxValue, skewEps, skewSeed)
      p should
        containInAnyOrder (Seq( ("a", (1, 11)),
                                ("a", (2, 11)),
                                ("b", (3, 12)),
                                ("b", (3, 13))))
    }
  }

  it should "support skewedJoin() with hotkey" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      // set threshold to 2, to hash join on "a"
      val p = p1.skewedJoin(p2, 2, skewEps, skewSeed)
      p should
        containInAnyOrder (Seq( ("a", (1, 11)),
                                ("a", (2, 11)),
                                ("b", (3, 12)),
                                ("b", (3, 13))))
    }
  }

  it should "support skewedJoin() with 0.5 sample" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("a", 3), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))

      // set threshold to 3, given 0.5 fraction for sample - "a" should not be hash joined
      val p = p1.skewedJoin(p2, 3, skewEps, skewSeed, sampleFraction = 0.5)
      p should
        containInAnyOrder (Seq( ("a", (1, 11)),
                                ("a", (2, 11)),
                                ("a", (3, 11)),
                                ("b", (3, 12)),
                                ("b", (3, 13))))
    }
  }

  it should "support skewedJoin() with empty key count (no hash join)" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2)))
      val p2 = sc.parallelize(Seq(("a", 11)))

      // set threshold to 3, given 0.5 fraction for sample - "a" should not be hash joined
      val p = p1.skewedJoin(p2, 3, skewEps, skewSeed, sampleFraction = 0.01)
      p should
        containInAnyOrder (Seq(("a", (2, 11)),
                               ("a", (1, 11))))
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

}
