package com.spotify.scio.named

import com.spotify.scio.testing.PipelineSpec

class NamedScioContextTest extends PipelineSpec {

  "NamedScioContext" should "support custom named transforms" in {
    import com.spotify.scio.named._

    runWithNamedContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
        .map(_ * 3) .| ("TripleInts")
        .filter(_ % 2 == 0) .| ("OnlyEven")
      p.internal.getProducingTransformInternal.getFullName shouldBe "OnlyEven/Filter"
      p should containInAnyOrder (Seq(6, 12))
    }
  }
}
