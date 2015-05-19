package com.spotify.cloud.dataflow.values

import com.spotify.cloud.dataflow.testing.PipelineTest

class SCollectionWithSideInputTest extends PipelineTest {

  def callSiteName(method: String) = s"$method SCollectionWithSideInputTest.scala:"

  "SCollectionWithSideInput" should "support filter()" in {
    runWithContext { pipeline =>
      val p1 = pipeline.parallelize(10, 11, 12)
      val p2 = pipeline.parallelize(1)
      val p3 = pipeline.parallelize(1, 2, 3)
      val p4 = pipeline.parallelize((10, 0), (11, 1), (12, 2))
      val s1 = p1.withSingletonSideInput(p2).filter((x, s) => (x + s) % 2 == 0).toSCollection
      val s2 = p1.withIterableSideInput(p3).filter((x, s) => (x + s.sum) % 2 == 0).toSCollection
      val s3 = p1.withMapSideInput(p4).filter((x, s) => (x + s(x).sum) % 2 == 0).toSCollection
      s1.internal should containInAnyOrder (11)
      s2.internal should containInAnyOrder (10, 12)
      s3.internal should containInAnyOrder (10, 11, 12)
    }
  }

  it should "support flatMap()" in {
    runWithContext { pipeline =>
      val p1 = pipeline.parallelize("a", "b", "c")
      val p2 = pipeline.parallelize(1)
      val p3 = pipeline.parallelize(1, 2, 3)
      val p4 = pipeline.parallelize(("a", 1), ("a", 2), ("b", 3), ("b", 4))
      val s1 = p1.withSingletonSideInput(p2).flatMap((x, s) => Seq(x + s + "x", x + s + "y")).toSCollection
      val s2 = p1.withIterableSideInput(p3).flatMap((x, s) => s.map(x + _)).toSCollection
      val s3 = p1.withMapSideInput(p4).flatMap((x, s) => s.getOrElse(x, Seq()).map(x + _)).toSCollection
      s1.internal should containInAnyOrder ("a1x", "b1x", "c1x", "a1y", "b1y", "c1y")
      s2.internal should containInAnyOrder ("a1", "b1", "c1", "a2", "b2", "c2", "a3", "b3", "c3")
      s3.internal should containInAnyOrder ("a1", "a2", "b3", "b4")
    }
  }

  it should "support keyBy()" in {
    runWithContext { pipeline =>
      val p1 = pipeline.parallelize("a", "b", "c")
      val p2 = pipeline.parallelize(1)
      val p3 = pipeline.parallelize(1, 2, 3)
      val p4 = pipeline.parallelize(("a", 1), ("b", 2), ("c", 3))
      val s1 = p1.withSingletonSideInput(p2).keyBy((x, s) => x + s).toSCollection
      val s2 = p1.withIterableSideInput(p3).keyBy((x, s) => x + s.sum).toSCollection
      val s3 = p1.withMapSideInput(p4).keyBy((x, s) => x + s(x).sum).toSCollection
      s1.internal should containInAnyOrder (("a1", "a"), ("b1", "b"), ("c1", "c"))
      s2.internal should containInAnyOrder (("a6", "a"), ("b6", "b"), ("c6", "c"))
      s3.internal should containInAnyOrder (("a1", "a"), ("b2", "b"), ("c3", "c"))
    }
  }

  it should "support map()" in {
    runWithContext { pipeline =>
      val p1 = pipeline.parallelize("a", "b", "c")
      val p2 = pipeline.parallelize(1)
      val p3 = pipeline.parallelize(1, 2, 3)
      val p4 = pipeline.parallelize(("a", 1), ("b", 2), ("c", 3))
      val s1 = p1.withSingletonSideInput(p2).map(_ + _).toSCollection
      val s2 = p1.withIterableSideInput(p3).map(_ + _.sum).toSCollection
      val s3 = p1.withMapSideInput(p4).map((x, s) => x + s(x).sum).toSCollection
      s1.internal should containInAnyOrder ("a1", "b1", "c1")
      s2.internal should containInAnyOrder ("a6", "b6", "c6")
      s3.internal should containInAnyOrder ("a1", "b2", "c3")
    }
  }

  it should "support chaining" in {
    runWithContext { pipeline =>
      val p1 = pipeline.parallelize("a", "b", "c")
      val p2 = pipeline.parallelize(1)
      val p3 = pipeline.parallelize(1, 2, 3)
      val p4 = pipeline.parallelize(("a", 1), ("b", 2), ("c", 3))
      val s1 = p1.withSingletonSideInput(p2)
        .filter((x, s) => x == "a")
        .flatMap((x, s) => Seq(x + s + "x", x + s + "y"))
        .keyBy((x, s) => "k" + s)
        .map((kv, s) => (kv._1, kv._2, s))
        .toSCollection
      val s2 = p1.withIterableSideInput(p3)
        .filter((x, s) => x == "a")
        .flatMap((x, s) => Seq(x + s.sum + "x", x + s.sum + "y"))
        .keyBy((x, s) => "k" + s.sum)
        .map((kv, s) => (kv._1, kv._2, s.sum))
        .toSCollection
      val s3 = p1.withMapSideInput(p4)
        .filter((x, s) => s(x).sum == 1)
        .flatMap((x, s) => Seq(x + s(x).sum + "x", x + s(x).sum + "y"))
        .keyBy((x, s) => "k" + s.values.flatten.sum)
        .map((kv, s) => (kv._1, kv._2, s.values.flatten.sum))
        .toSCollection
      s1.internal should containInAnyOrder (("k1", "a1x", 1), ("k1", "a1y", 1))
      s2.internal should containInAnyOrder (("k6", "a6x", 6), ("k6", "a6y", 6))
      s3.internal should containInAnyOrder (("k6", "a1x", 6), ("k6", "a1y", 6))
    }
  }

}
