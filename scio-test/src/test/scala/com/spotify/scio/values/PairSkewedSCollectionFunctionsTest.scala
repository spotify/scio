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

class PairSkewedSCollectionFunctionsTest extends PipelineSpec {
  val (skewSeed, skewEps) = (42, 0.001d)

  it should "support skewedJoin() without hotkeys and no duplicate keys" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.skewedJoin(p2, Long.MaxValue, skewEps, skewSeed)
      p should containInAnyOrder(Seq(("a", (1, 11)), ("b", (2, 12)), ("b", (2, 13))))
    }
  }

  it should "support skewedJoin() without hotkeys" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.skewedJoin(p2, Long.MaxValue, skewEps, skewSeed)
      p should containInAnyOrder(
        Seq(("a", (1, 11)), ("a", (2, 11)), ("b", (3, 12)), ("b", (3, 13)))
      )
    }
  }

  it should "support skewedJoin() with hotkey" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      // set threshold to 2, to hash join on "a"
      val p = p1.skewedJoin(p2, 2, skewEps, skewSeed)
      p should containInAnyOrder(
        Seq(("a", (1, 11)), ("a", (2, 11)), ("b", (3, 12)), ("b", (3, 13)))
      )
    }
  }

  it should "support skewedJoin() with 0.5 sample" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("a", 3), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))

      // set threshold to 3, given 0.5 fraction for sample - "a" should not be hash joined
      val p = p1.skewedJoin(p2, 3, skewEps, skewSeed, sampleFraction = 0.5)
      p should containInAnyOrder(
        Seq(("a", (1, 11)), ("a", (2, 11)), ("a", (3, 11)), ("b", (3, 12)), ("b", (3, 13)))
      )
    }
  }

  it should "support skewedJoin() with empty key count (no hash join)" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2)))
      val p2 = sc.parallelize(Seq(("a", 11)))

      // Small sample size to force empty key count
      val p = p1.skewedJoin(p2, 3, skewEps, skewSeed, sampleFraction = 0.01)
      p should containInAnyOrder(Seq(("a", (2, 11)), ("a", (1, 11))))
    }
  }

  it should "support skewedLeftOuterJoin() without hotkeys and no duplicate keys" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.skewedLeftOuterJoin(p2, Long.MaxValue, skewEps, skewSeed)
      p should containInAnyOrder(
        Seq(("a", (1, Some(11))), ("b", (2, Some(12))), ("b", (2, Some(13))), ("c", (3, None)))
      )
    }
  }

  it should "support skewedLeftOuterJoin() without hotkeys" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.skewedLeftOuterJoin(p2, Long.MaxValue, skewEps, skewSeed)
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

  it should "support skewedLeftOuterJoin() with hotkey" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      // set threshold to 2, to hash join on "a"
      val p = p1.skewedLeftOuterJoin(p2, 2, skewEps, skewSeed)
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

  it should "support skewedLeftOuterJoin() with 0.5 sample" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 =
        sc.parallelize(Seq(("a", 1), ("a", 2), ("a", 3), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))

      // set threshold to 3, given 0.5 fraction for sample - "a" should not be hash joined
      val p = p1.skewedLeftOuterJoin(p2, 3, skewEps, skewSeed, sampleFraction = 0.5)
      p should containInAnyOrder(
        Seq(
          ("a", (1, Some(11))),
          ("a", (2, Some(11))),
          ("a", (3, Some(11))),
          ("b", (3, Some(12))),
          ("b", (3, Some(13))),
          ("c", (4, None))
        )
      )
    }
  }

  it should "support skewedLeftOuterJoin() with empty key count (no hash join)" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11)))

      // Small sample size to force empty key count
      val p = p1.skewedLeftOuterJoin(p2, 3, skewEps, skewSeed, sampleFraction = 0.01)
      p should containInAnyOrder(Seq(("a", (2, Some(11))), ("a", (1, Some(11))), ("b", (3, None))))
    }
  }

  it should "support skewedFullOuterJoin() without hotkeys and no duplicate keys" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = p1.skewedFullOuterJoin(p2, Long.MaxValue, skewEps, skewSeed)
      p should containInAnyOrder(
        Seq(
          ("a", (Some(1), Some(11))),
          ("b", (Some(2), Some(12))),
          ("b", (Some(2), Some(13))),
          ("c", (Some(3), None)),
          ("d", (None, Some(14)))
        )
      )
    }
  }

  it should "support skewedFullOuterJoin() without hotkeys" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = p1.skewedFullOuterJoin(p2, Long.MaxValue, skewEps, skewSeed)
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

  it should "support skewedFullOuterJoin() with hotkey" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      // set threshold to 2, to hash join on "a"
      val p = p1.skewedFullOuterJoin(p2, 2, skewEps, skewSeed)
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

  it should "support skewedFullOuterJoin() with 0.5 sample" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 =
        sc.parallelize(Seq(("a", 1), ("a", 2), ("a", 3), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))

      // set threshold to 3, given 0.5 fraction for sample - "a" should not be hash joined
      val p =
        p1.skewedFullOuterJoin(p2, 3, skewEps, skewSeed, sampleFraction = 0.5)
      p should containInAnyOrder(
        Seq(
          ("a", (Some(1), Some(11))),
          ("a", (Some(2), Some(11))),
          ("a", (Some(3), Some(11))),
          ("b", (Some(3), Some(12))),
          ("b", (Some(3), Some(13))),
          ("c", (Some(4), None)),
          ("d", (None, Some(14)))
        )
      )
    }
  }

  it should "support skewedFullOuterJoin() with empty key count (no hash join)" in {
    import com.twitter.algebird.CMSHasherImplicits._
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("c", 12)))

      // Small sample size to force empty key count
      val p =
        p1.skewedFullOuterJoin(p2, 3, skewEps, skewSeed, sampleFraction = 0.01)
      p should containInAnyOrder(
        Seq(
          ("a", (Some(2), Some(11))),
          ("a", (Some(1), Some(11))),
          ("b", (Some(3), None)),
          ("c", (None, Some(12)))
        )
      )
    }
  }
}
