package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec

class SCollectionWithSideOutputTest extends PipelineSpec {

  "SCollectionWithSideOutput" should "support map()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = SideOutput[String]()
      val (main, side) = p1.withSideOutputs(p2).map { (x, s) => s.output(p2, x + "2"); x + "1" }
      main.internal should containInAnyOrder (Seq("a1", "b1", "c1"))
      side(p2).internal should containInAnyOrder (Seq("a2", "b2", "c2"))
    }
  }

  it should "support flatMap()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = SideOutput[String]()
      val (main, side) = p1.withSideOutputs(p2).flatMap { (x, s) =>
        s.output(p2, x + "2x").output(p2, x + "2y")
        Seq(x + "1x", x + "1y")
      }
      main.internal should containInAnyOrder (Seq("a1x", "a1y", "b1x", "b1y", "c1x", "c1y"))
      side(p2).internal should containInAnyOrder (Seq("a2x", "a2y", "b2x", "b2y", "c2x", "c2y"))
    }
  }

}
