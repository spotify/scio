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
import com.twitter.algebird.{CMS, TopNCMS, TopPctCMS}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PairSkewedSCollectionFunctionsTest extends PipelineSpec with ScalaCheckPropertyChecks {
  import com.twitter.algebird.CMSHasherImplicits._

  private val skewSeed = 42
  private val skewEps = 0.001d
  private val skewDelta = 1e-10
  private val skewWithReplacement = true

  private val joinSetup = Table(
    ("lhs", "rhs", "out"),
    // no duplicate keys
    (
      Seq("a" -> 1, "b" -> 2, "c" -> 3),
      Seq("a" -> 11, "b" -> 12, "b" -> 13),
      Seq("a" -> ((1, 11)), "b" -> ((2, 12)), "b" -> ((2, 13)))
    ),
    // duplicate keys
    (
      Seq("a" -> 1, "a" -> 2, "b" -> 3),
      Seq("a" -> 11, "b" -> 12, "b" -> 13, "c" -> 14),
      Seq("a" -> ((1, 11)), "a" -> ((2, 11)), "b" -> ((3, 12)), "b" -> ((3, 13)))
    )
  )

  private val leftJoinSetup = Table(
    ("lhs", "rhs", "out"),
    // no duplicate keys
    (
      Seq("a" -> 1, "b" -> 2, "c" -> 3),
      Seq("a" -> 11, "b" -> 12, "b" -> 13),
      Seq(
        "a" -> ((1, Some(11))),
        "b" -> ((2, Some(12))),
        "b" -> ((2, Some(13))),
        "c" -> ((3, None))
      )
    ),
    // duplicate keys
    (
      Seq("a" -> 1, "a" -> 2, "b" -> 3),
      Seq("a" -> 11, "b" -> 12, "b" -> 13, "c" -> 14),
      Seq(
        "a" -> ((1, Some(11))),
        "a" -> ((2, Some(11))),
        "b" -> ((3, Some(12))),
        "b" -> ((3, Some(13)))
      )
    )
  )

  private val fullJoinSetup = Table(
    ("lhs", "rhs", "out"),
    // no duplicate keys
    (
      Seq("a" -> 1, "b" -> 2, "c" -> 3),
      Seq("a" -> 11, "b" -> 12, "b" -> 13),
      Seq(
        "a" -> ((Some(1), Some(11))),
        "b" -> ((Some(2), Some(12))),
        "b" -> ((Some(2), Some(13))),
        "c" -> ((Some(3), None))
      )
    ),
    // duplicate keys
    (
      Seq("a" -> 1, "a" -> 2, "b" -> 3),
      Seq("a" -> 11, "b" -> 12, "b" -> 13, "c" -> 14),
      Seq(
        "a" -> ((Some(1), Some(11))),
        "a" -> ((Some(2), Some(11))),
        "b" -> ((Some(3), Some(12))),
        "b" -> ((Some(3), Some(13))),
        "c" -> ((None, Some(14)))
      )
    )
  )

  private val methodSetup = Table(
    "method",
    // no hot keys
    HotKeyMethod.Threshold(Long.MaxValue),
    HotKeyMethod.TopPercentage(0.99),
    // hot keys
    HotKeyMethod.Threshold(2),
    HotKeyMethod.TopPercentage(0.25),
    HotKeyMethod.TopN(2)
  )

  private val sampleSetup = Table(
    "sample",
    1.0,
    0.5
  )

  it should "support skewedJoin()" in {
    forAll(joinSetup) { case (lhs, rhs, out) =>
      forAll(sampleSetup) { sampleFraction =>
        forAll(methodSetup) { method =>
          runWithContext { sc =>
            val pLhs = sc.parallelize(lhs)
            val pRhs = sc.parallelize(rhs)
            val p =
              pLhs.skewedJoin(
                pRhs,
                method,
                skewEps,
                skewDelta,
                skewSeed,
                sampleFraction,
                skewWithReplacement
              )
            p should containInAnyOrder(out)
          }
        }
      }
    }
  }

  it should "support skewedLeftOuterJoin()" in {
    forAll(leftJoinSetup) { case (lhs, rhs, out) =>
      forAll(sampleSetup) { sampleFraction =>
        forAll(methodSetup) { method =>
          runWithContext { sc =>
            val pLhs = sc.parallelize(lhs)
            val pRhs = sc.parallelize(rhs)
            val p =
              pLhs.skewedLeftOuterJoin(
                pRhs,
                method,
                skewEps,
                skewDelta,
                skewSeed,
                sampleFraction,
                skewWithReplacement
              )
            p should containInAnyOrder(out)
          }
        }
      }
    }
  }

  it should "support skewedFullOuterJoin()" in {
    forAll(fullJoinSetup) { case (lhs, rhs, out) =>
      forAll(sampleSetup) { sampleFraction =>
        forAll(methodSetup) { method =>
          runWithContext { sc =>
            val pLhs = sc.parallelize(lhs)
            val pRhs = sc.parallelize(rhs)
            val p = pLhs.skewedFullOuterJoin(
              pRhs,
              method,
              skewEps,
              skewDelta,
              skewSeed,
              sampleFraction,
              skewWithReplacement
            )
            p should containInAnyOrder(out)
          }
        }
      }
    }
  }

  it should "partition hot keys with threshold method" in {
    val lhs = Seq("a" -> 1, "a" -> 2, "a" -> 3, "b" -> 4)
    val rhs = Seq("a" -> 11, "b" -> 12, "c" -> 13)
    val threshold = 2L
    runWithContext { sc =>
      val pLhs = sc.parallelize(lhs)
      val pRhs = sc.parallelize(rhs)
      val aggregator = CMS.aggregator[String](skewEps, skewDelta, skewSeed)
      val cms = CMSOperations.aggregate(pLhs.map(_._1), aggregator)
      val (l, r) = CMSOperations.partition(pLhs, pRhs, cms, threshold)

      // hot key is a
      val (lhot, lchill) = lhs.partition { case (k, _) => k == "a" }
      l.hot should containInAnyOrder(lhot)
      l.chill should containInAnyOrder(lchill)

      val (rhot, rchill) = rhs.partition { case (k, _) => k == "a" }
      r.hot should containInAnyOrder(rhot)
      r.chill should containInAnyOrder(rchill)
    }
  }

  it should "partition hot keys with percentage method" in {
    val lhs = Seq("a" -> 1, "a" -> 2, "a" -> 3, "b" -> 4)
    val rhs = Seq("a" -> 11, "b" -> 12, "c" -> 13)
    val pct = 0.5
    runWithContext { sc =>
      val pLhs = sc.parallelize(lhs)
      val pRhs = sc.parallelize(rhs)
      val aggregator = TopPctCMS.aggregator[String](skewEps, skewDelta, skewSeed, pct)
      val cms = CMSOperations.aggregate(pLhs.map(_._1), aggregator)
      val (l, r) = CMSOperations.partition(pLhs, pRhs, cms)

      // hot key is a
      val (lhot, lchill) = lhs.partition { case (k, _) => k == "a" }
      l.hot should containInAnyOrder(lhot)
      l.chill should containInAnyOrder(lchill)

      val (rhot, rchill) = rhs.partition { case (k, _) => k == "a" }
      r.hot should containInAnyOrder(rhot)
      r.chill should containInAnyOrder(rchill)
    }
  }

  it should "partition hot keys with topN method" in {
    val lhs = Seq("a" -> 1, "a" -> 2, "a" -> 3, "b" -> 4)
    val rhs = Seq("a" -> 11, "b" -> 12, "c" -> 13)
    val count = 1
    runWithContext { sc =>
      val pLhs = sc.parallelize(lhs)
      val pRhs = sc.parallelize(rhs)
      val aggregator = TopNCMS.aggregator[String](skewEps, skewDelta, skewSeed, count)
      val cms = CMSOperations.aggregate(pLhs.map(_._1), aggregator)
      val (l, r) = CMSOperations.partition(pLhs, pRhs, cms)

      // hot key is a
      val (lhot, lchill) = lhs.partition { case (k, _) => k == "a" }
      l.hot should containInAnyOrder(lhot)
      l.chill should containInAnyOrder(lchill)

      val (rhot, rchill) = rhs.partition { case (k, _) => k == "a" }
      r.hot should containInAnyOrder(rhot)
      r.chill should containInAnyOrder(rchill)
    }
  }
}
