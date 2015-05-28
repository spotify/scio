package com.spotify.cloud.dataflow.values

import com.spotify.cloud.dataflow.testing.PipelineTest
import com.twitter.algebird.Aggregator

class PairSCollectionFunctionsTest extends PipelineTest {

  "PairSCollection" should "support coGroup()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11L), ("b", 12L), ("d", 14L))
      val r1 = p1.coGroup(p2)
      val r2 = p1.groupWith(p2)
      val expected = Seq(
        ("a", (iterable(1), iterable(11L))),
        ("b", (iterable(2), iterable(12L))),
        ("c", (iterable(3), iterable())),
        ("d", (iterable(), iterable(14L))))
      r1.internal should equalInAnyOrder (expected)
      r2.internal should equalInAnyOrder (expected)
    }
  }

  it should "support coGroup() with duplicate keys" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("a", 2), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11L), ("b", 12L), ("b", 13L), ("d", 14L))
      def fn = (t: (String, (Iterable[Int], Iterable[Long]))) => (t._1, (t._2._1.toSet, t._2._2.toSet))
      val r1 = p1.coGroup(p2).map(fn)
      val r2 = p1.groupWith(p2).map(fn)
      val expected = Seq[(String, (Set[Int], Set[Long]))](
        ("a", (Set(1, 2), Set(11L))),
        ("b", (Set(2), Set(12L, 13L))),
        ("c", (Set(3), Set())),
        ("d", (Set(), Set(14L))))
      r1.internal should equalInAnyOrder (expected)
      r2.internal should equalInAnyOrder (expected)
    }
  }

  it should "support 3-way coGroup()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11L), ("b", 12L), ("d", 14L))
      val p3 = context.parallelize(("a", 21F), ("b", 22F), ("e", 25F))
      val r1 = p1.coGroup(p2, p3)
      val r2 = p1.groupWith(p2, p3)
      val expected = Seq(
        ("a", (iterable(1), iterable(11L), iterable(21F))),
        ("b", (iterable(2), iterable(12L), iterable(22F))),
        ("c", (iterable(3), iterable(), iterable())),
        ("d", (iterable(), iterable(14L), iterable())),
        ("e", (iterable(), iterable(), iterable(25F))))
      r1.internal should equalInAnyOrder (expected)
      r2.internal should equalInAnyOrder (expected)
    }
  }

  it should "support 4-way coGroup()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11L), ("b", 12L), ("d", 14L))
      val p3 = context.parallelize(("a", 21F), ("b", 22F), ("e", 25F))
      val p4 = context.parallelize(("a", 31.0), ("b", 32.0), ("f", 36.0))
      val r1 = p1.coGroup(p2, p3, p4)
      val r2 = p1.groupWith(p2, p3, p4)
      val expected = Seq(
        ("a", (iterable(1), iterable(11L), iterable(21F), iterable(31.0))),
        ("b", (iterable(2), iterable(12L), iterable(22F), iterable(32.0))),
        ("c", (iterable(3), iterable(), iterable(), iterable())),
        ("d", (iterable(), iterable(14L), iterable(), iterable())),
        ("e", (iterable(), iterable(), iterable(25F), iterable())),
        ("f", (iterable(), iterable(), iterable(), iterable(36.0))))
      r1.internal should equalInAnyOrder (expected)
      r2.internal should equalInAnyOrder (expected)
    }
  }

  it should "support fullOuterJoin()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("d", 14))
      val p = p1.fullOuterJoin(p2)
      p.internal should containInAnyOrder (
        ("a", (Some(1), Some(11))),
        ("b", (Some(2), Some(12))),
        ("c", (Some(3), None)),
        ("d", (None, Some(14))))
    }
  }

  it should "support fullOuterJoin() with duplicate keys" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("a", 2), ("b", 3), ("c", 4))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("b", 13), ("d", 14))
      val p = p1.fullOuterJoin(p2)
      p.internal should containInAnyOrder (
        ("a", (Some(1), Some(11))),
        ("a", (Some(2), Some(11))),
        ("b", (Some(3), Some(12))),
        ("b", (Some(3), Some(13))),
        ("c", (Some(4), None)),
        ("d", (None, Some(14))))
    }
  }

  it should "support join()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("d", 14))
      val p = p1.join(p2)
      p.internal should containInAnyOrder (("a", (1, 11)), ("b", (2, 12)))
    }
  }

  it should "support leftOuterJoin()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("d", 14))
      val p = p1.leftOuterJoin(p2)
      p.internal should containInAnyOrder (("a", (1, Some(11))), ("b", (2, Some(12))), ("c", (3, None)))
    }
  }

  it should "support leftOuterJoin() with duplicate keys" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("a", 2), ("b", 3), ("c", 4))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("b", 13), ("d", 14))
      val p = p1.leftOuterJoin(p2)
      p.internal should containInAnyOrder (
        ("a", (1, Some(11))),
        ("a", (2, Some(11))),
        ("b", (3, Some(12))),
        ("b", (3, Some(13))),
        ("c", (4, None)))
    }
  }

  it should "support rightOuterJoin()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("d", 14))
      val p = p1.rightOuterJoin(p2)
      p.internal should containInAnyOrder (("a", (Some(1), 11)), ("b", (Some(2), 12)), ("d", (None, 14)))
    }
  }

  it should "support rightOuterJoin() with duplicate keys" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("a", 2), ("b", 3), ("c", 4))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("b", 13), ("d", 14))
      val p = p1.rightOuterJoin(p2)
      p.internal should containInAnyOrder (
        ("a", (Some(1), 11)),
        ("a", (Some(2), 11)),
        ("b", (Some(3), 12)),
        ("b", (Some(3), 13)),
        ("d", (None, 14)))
    }
  }

  it should "support aggregateByKey()" in {
    runWithContext { context =>
      val p1 = context.parallelize(1 to 100: _*).map(("a", _))
      val p2 = context.parallelize(1 to 10: _*).map(("b", _))
      val r1 = (p1 ++ p2).aggregateByKey(0.0)(_ + _, _ + _)
      val r2 = (p1 ++ p2).aggregateByKey(Aggregator.max[Int])
      val r3 = (p1 ++ p2).aggregateByKey(Aggregator.sortedReverseTake[Int](5))
      r1.internal should containInAnyOrder (("a", 5050.0), ("b", 55.0))
      r2.internal should containInAnyOrder (("a", 100), ("b", 10))
      r3.internal should containInAnyOrder (("a", Seq(100, 99, 98, 97, 96)), ("b", Seq(10, 9, 8, 7, 6)))
    }
  }

  it should "support approxQuantilesByKey()" in {
    runWithContext { context =>
      val p1 = context.parallelize(0 to 100: _*).map(("a", _))
      val p2 = context.parallelize(0 to 10: _*).map(("b", _))
      val p = (p1 ++ p2).approxQuantilesByKey(3)
      p.internal should containInAnyOrder (("a", iterable(0, 50, 100)), ("b", iterable(0, 5, 10)))
    }
  }

  it should "support combineByKey()" in {
    runWithContext { context =>
      val p1 = context.parallelize(1 to 100: _*).map(("a", _))
      val p2 = context.parallelize(1 to 10: _*).map(("b", _))
      val p = (p1 ++ p2).combineByKey(_.toDouble)(_ + _)(_ + _)
      p.internal should containInAnyOrder (("a", 5050.0), ("b", 55.0))
    }
  }

  it should "support countApproxDistinctByKey()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 11), ("a", 12), ("b", 21), ("b", 22), ("b", 23))
      val r1 = p.countApproxDistinctByKey()
      val r2 = p.countApproxDistinctByKey(sampleSize = 10000)
      r1.internal should containInAnyOrder (("a", 2L), ("b", 3L))
      r2.internal should containInAnyOrder (("a", 2L), ("b", 3L))
    }
  }

  it should "support countByKey()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 11), ("a", 12), ("b", 21), ("b", 22), ("b", 23)).countByKey()
      p.internal should containInAnyOrder (("a", 2L), ("b", 3L))
    }
  }

  it should "support flatMapValues()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("b", 2)).flatMapValues(v => Seq(v + 10.0, v + 20.0))
      p.internal should containInAnyOrder (("a", 11.0), ("a", 21.0), ("b", 12.0), ("b", 22.0))
    }
  }

  it should "support foldByKey()" in {
    runWithContext { context =>
      val p1 = context.parallelize(1 to 100: _*).map(("a", _))
      val p2 = context.parallelize(1 to 10: _*).map(("b", _))
      val r1 = (p1 ++ p2).foldByKey(0)(_ + _)
      val r2 = (p1 ++ p2).foldByKey
      r1.internal should containInAnyOrder (("a", 5050), ("b", 55))
      r2.internal should containInAnyOrder (("a", 5050), ("b", 55))
    }
  }

  it should "support groupByKey()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("a", 10), ("b", 2), ("b", 20)).groupByKey().mapValues(_.toSet)
      p.internal should containInAnyOrder (("a", Set(1, 10)), ("b", Set(2, 20)))
    }
  }

  it should "support keys()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("b", 2), ("c", 3)).keys
      p.internal should containInAnyOrder ("a", "b", "c")
    }
  }

  it should "support mapValues()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("b", 2)).mapValues(_ + 10.0)
      p.internal should containInAnyOrder (("a", 11.0), ("b", 12.0))
    }
  }

  it should "support maxByKey()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("a", 10), ("b", 2), ("b", 20)).maxByKey()
      p.internal should containInAnyOrder (("a", 10), ("b", 20))
    }
  }

  it should "support minByKey()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("a", 10), ("b", 2), ("b", 20)).minByKey()
      p.internal should containInAnyOrder (("a", 1), ("b", 2))
    }
  }

  it should "support reduceByKey()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("b", 1), ("b", 2), ("c", 1), ("c", 2), ("c", 3)).reduceByKey(_ + _)
      p.internal should containInAnyOrder (("a", 1), ("b", 3), ("c", 6))
    }
  }

  it should "support sampleByKey()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("b", 2), ("b", 2), ("c", 3), ("c", 3), ("c", 3)).sampleByKey(1)
      p.internal should containInAnyOrder (("a", iterable(1)), ("b", iterable(2)), ("c", iterable(3)))
    }
  }

  ignore should "support sampleByKey() with replacement()" in {
    runWithContext { context =>
      import RandomSamplerUtils._
      verifyByKey(context, true, 0.5, 0.5, 0.9, 0.9).internal should containSingleValue ((true, true))
      verifyByKey(context, true, 0.9, 0.9, 0.4, 0.6).internal should containSingleValue ((true, false))
      verifyByKey(context, true, 0.4, 0.6, 0.9, 0.9).internal should containSingleValue ((false, true))
      verifyByKey(context, true, 0.4, 0.6, 0.4, 0.6).internal should containSingleValue ((false, false))
    }
  }

  ignore should "support sampleByKey() without replacement()" in {
    runWithContext { context =>
      import RandomSamplerUtils._
      verifyByKey(context, false, 0.5, 0.5, 0.9, 0.9).internal should containSingleValue ((true, true))
      verifyByKey(context, false, 0.9, 0.9, 0.4, 0.6).internal should containSingleValue ((true, false))
      verifyByKey(context, false, 0.4, 0.6, 0.9, 0.9).internal should containSingleValue ((false, true))
      verifyByKey(context, false, 0.4, 0.6, 0.4, 0.6).internal should containSingleValue ((false, false))
    }
  }

  it should "support subtractByKey()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("b", 3), ("c", 4), ("c", 5), ("c", 6))
      val p2 = context.parallelize(("a", 10L), ("b", 20L))
      val p = p1.subtractByKey(p2)
      p.internal should containInAnyOrder (("c", 4), ("c", 5), ("c", 6))
    }
  }

  it should "support sumByKey()" in {
    runWithContext { context =>
      val p = context.parallelize(List(("a", 1), ("b", 2), ("b", 2)) ++ (1 to 100).map(("c", _)): _*).sumByKey()
      p.internal should containInAnyOrder (("a", 1), ("b", 4), ("c", 5050))
    }
  }

  it should "support swap()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("b", 2), ("c", 3)).swap
      p.internal should containInAnyOrder ((1, "a"), (2, "b"), (3, "c"))
    }
  }

  it should "support topByKey()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("b", 11), ("b", 12), ("c", 21), ("c", 22), ("c", 23))
      val r1 = p.topByKey(1)
      val r2 = p.topByKey(1)(Ordering.by(-_))
      r1.internal should containInAnyOrder (("a", iterable(1)), ("b", iterable(12)), ("c", iterable(23)))
      r2.internal should containInAnyOrder (("a", iterable(1)), ("b", iterable(11)), ("c", iterable(21)))
    }
  }

  it should "support values()" in {
    runWithContext { context =>
      val p = context.parallelize(("a", 1), ("b", 2), ("c", 3)).values
      p.internal should containInAnyOrder (1, 2, 3)
    }
  }

  it should "support hashJoin()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("d", 14))
      val p = p1.hashJoin(p2)
      p.internal should containInAnyOrder (("a", (1, 11)), ("b", (2, 12)))
    }
  }

  it should "support hashJoin() with duplicate keys" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("a", 2), ("b", 3))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("b", 13))
      val p = p1.hashJoin(p2)
      p.internal should containInAnyOrder (("a", (1, 11)), ("a", (2, 11)), ("b", (3, 12)), ("b", (3, 13)))
    }
  }

  it should "support hashLeftJoin()" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("b", 2), ("c", 3))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("d", 14))
      val p = p1.hashLeftJoin(p2)
      p.internal should containInAnyOrder (("a", (1, Some(11))), ("b", (2, Some(12))), ("c", (3, None)))
    }
  }

  it should "support hashLeftJoin() with duplicate keys" in {
    runWithContext { context =>
      val p1 = context.parallelize(("a", 1), ("a", 2), ("b", 3), ("c", 4))
      val p2 = context.parallelize(("a", 11), ("b", 12), ("b", 13))
      val p = p1.hashLeftJoin(p2)
      p.internal should containInAnyOrder (
        ("a", (1, Some(11))),
        ("a", (2, Some(11))),
        ("b", (3, Some(12))),
        ("b", (3, Some(13))),
        ("c", (4, None)))
    }
  }

}
