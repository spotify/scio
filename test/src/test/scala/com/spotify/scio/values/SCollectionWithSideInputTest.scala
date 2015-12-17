package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec

// TODO: refactor tests and cover ListSideInput
class SCollectionWithSideInputTest extends PipelineSpec {

  "SCollectionWithSideInput" should "support filter()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(10, 11, 12))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val p3 = sc.parallelize(Seq(1, 2, 3)).asIterableSideInput
      val p4 = sc.parallelize(Seq((10, 0), (11, 1), (12, 2))).asMultiMapSideInput
      val s1 = p1.withSideInputs(p2).filter((x, s) => (x + s(p2)) % 2 == 0)
      val s2 = p1.withSideInputs(p3).filter((x, s) => (x + s(p3).sum) % 2 == 0)
      val s3 = p1.withSideInputs(p4).filter((x, s) => (x + s(p4)(x).sum) % 2 == 0)
      val s4 = p1.withSideInputs(p2, p3, p4).filter((x, s) => (x + s(p2) + s(p3).sum + s(p4)(x).sum) % 2 == 1)
      s1.internal should containInAnyOrder (Seq(11))
      s2.internal should containInAnyOrder (Seq(10, 12))
      s3.internal should containInAnyOrder (Seq(10, 11, 12))
      s4.internal should containInAnyOrder (Seq(10, 11, 12))
    }
  }

  it should "support flatMap()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val p3 = sc.parallelize(Seq(1, 2, 3)).asIterableSideInput
      val p4 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("b", 4))).asMultiMapSideInput
      val s1 = p1.withSideInputs(p2).flatMap((x, s) => Seq(x + s(p2) + "x", x + s(p2) + "y"))
      val s2 = p1.withSideInputs(p3).flatMap((x, s) => s(p3).map(x + _))
      val s3 = p1.withSideInputs(p4).flatMap((x, s) => s(p4).getOrElse(x, Nil).map(x + _))
      val s4 = p1.withSideInputs(p2, p3, p4).flatMap { (x, s) =>
        s(p4).getOrElse(x, Nil).map(i => x + (i + s(p2) + s(p3).sum))
      }
      s1.internal should containInAnyOrder (Seq("a1x", "b1x", "c1x", "a1y", "b1y", "c1y"))
      s2.internal should containInAnyOrder (Seq("a1", "b1", "c1", "a2", "b2", "c2", "a3", "b3", "c3"))
      s3.internal should containInAnyOrder (Seq("a1", "a2", "b3", "b4"))
      s4.internal should containInAnyOrder (Seq("a8", "a9", "b10", "b11"))
    }
  }

  it should "support keyBy()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val p3 = sc.parallelize(Seq(1, 2, 3)).asIterableSideInput
      val p4 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).asMultiMapSideInput
      val s1 = p1.withSideInputs(p2).keyBy(_ + _(p2))
      val s2 = p1.withSideInputs(p3).keyBy(_ + _(p3).sum)
      val s3 = p1.withSideInputs(p4).keyBy((x, s) => x + s(p4)(x).sum)
      val s4 = p1.withSideInputs(p2, p3, p4).keyBy((x, s) => x + (s(p2) + s(p3).sum + s(p4)(x).sum))
      s1.internal should containInAnyOrder (Seq(("a1", "a"), ("b1", "b"), ("c1", "c")))
      s2.internal should containInAnyOrder (Seq(("a6", "a"), ("b6", "b"), ("c6", "c")))
      s3.internal should containInAnyOrder (Seq(("a1", "a"), ("b2", "b"), ("c3", "c")))
      s4.internal should containInAnyOrder (Seq(("a8", "a"), ("b9", "b"), ("c10", "c")))
    }
  }

  it should "support map()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val p3 = sc.parallelize(Seq(1, 2, 3)).asIterableSideInput
      val p4 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).asMultiMapSideInput
      val s1 = p1.withSideInputs(p2).map(_ + _(p2))
      val s2 = p1.withSideInputs(p3).map(_ + _(p3).sum)
      val s3 = p1.withSideInputs(p4).map((x, s) => x + s(p4)(x).sum)
      val s4 = p1.withSideInputs(p2, p3, p4).map((x, s) => x + (s(p2) + s(p3).sum + s(p4)(x).sum))
      s1.internal should containInAnyOrder (Seq("a1", "b1", "c1"))
      s2.internal should containInAnyOrder (Seq("a6", "b6", "c6"))
      s3.internal should containInAnyOrder (Seq("a1", "b2", "c3"))
      s4.internal should containInAnyOrder (Seq("a8", "b9", "c10"))
    }
  }

  it should "support chaining" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val p3 = sc.parallelize(Seq(1, 2, 3)).asIterableSideInput
      val p4 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).asMultiMapSideInput
      val s1 = p1.withSideInputs(p2)
        .filter((x, s) => x == "a")
        .flatMap((x, s) => Seq(x + s(p2) + "x", x + s(p2) + "y"))
        .keyBy((x, s) => "k" + s(p2))
        .map((kv, s) => (kv._1, kv._2, s(p2)))
        .toSCollection
      val s2 = p1.withSideInputs(p3)
        .filter((x, s) => x == "a")
        .flatMap((x, s) => Seq(x + s(p3).sum + "x", x + s(p3).sum + "y"))
        .keyBy((x, s) => "k" + s(p3).sum)
        .map((kv, s) => (kv._1, kv._2, s(p3).sum))
        .toSCollection
      val s3 = p1.withSideInputs(p4)
        .filter((x, s) => s(p4)(x).sum == 1)
        .flatMap((x, s) => Seq(x + s(p4)(x).sum + "x", x + s(p4)(x).sum + "y"))
        .keyBy((x, s) => "k" + s(p4).values.flatten.sum)
        .map((kv, s) => (kv._1, kv._2, s(p4).values.flatten.sum))
        .toSCollection
      s1.internal should containInAnyOrder (Seq(("k1", "a1x", 1), ("k1", "a1y", 1)))
      s2.internal should containInAnyOrder (Seq(("k6", "a6x", 6), ("k6", "a6y", 6)))
      s3.internal should containInAnyOrder (Seq(("k6", "a1x", 6), ("k6", "a1y", 6)))
    }
  }

}
