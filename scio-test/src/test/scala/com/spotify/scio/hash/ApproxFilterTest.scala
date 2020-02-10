/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.hash

import com.spotify.scio.coders.CoderMaterializer
import com.spotify.scio.testing._
import magnolify.guava.auto._
import org.apache.beam.sdk.util.{CoderUtils, SerializableUtils}

class ApproxFilterTest extends PipelineSpec {
  def test[C <: ApproxFilterCompanion](c: C)(implicit hash: c.Hash[Int]): Unit = {
    val filterName = c.getClass.getSimpleName.stripSuffix("$")

    // make `expectedInsertions` to number of unique items ratio 1.1 to prevent filter saturation
    // see [[com.twitter.algebird.BF.contains]] for the empirical factor
    val paddedInput = (1 to 1000) ++ (1 to 100)

    filterName should "work with defaults" in {
      val bf = c.create(paddedInput)
      bf.approxElementCount should be <= 1000L
      bf.expectedFpp should be <= 0.03
      // no false negatives
      (1 to 1000).forall(bf.mightContain) shouldBe true
      // true fpp
      (1001 to 2000).count(bf.mightContain).toDouble / 1000 should be <= 0.03
      ()
    }

    it should "work with custom expectedInsertions" in {
      val bf = c.create(paddedInput, 2000)
      bf.approxElementCount should be <= 2000L
      bf.expectedFpp should be <= 0.03
      (1 to 1000).forall(bf.mightContain) shouldBe true
      (1001 to 2000).count(bf.mightContain).toDouble / 1000 should be <= 0.03
      ()
    }

    it should "work with custom fpp" in {
      val bf = c.create(paddedInput, 2000, 0.01)
      bf.approxElementCount should be <= 2000L
      bf.expectedFpp should be <= 0.01
      (1 to 1000).forall(bf.mightContain) shouldBe true
      (1001 to 2000).count(bf.mightContain).toDouble / 1000 should be <= 0.01
      ()
    }

    it should "work with SCollection" in {
      runWithContext { sc =>
        implicit val coder = c.coder
        c.create(sc.parallelize(paddedInput)) should satisfySingleValue[c.Filter[Int]] { bf1 =>
          val bf2 = c.create(paddedInput)
          eq(bf1, bf2)
        }
      }
    }

    it should "support Iterable syntax" in {
      eq(paddedInput.asApproxFilter(c), c.create(paddedInput))
      eq(paddedInput.asApproxFilter(c, 2000), c.create(paddedInput, 2000))
      eq(paddedInput.asApproxFilter(c, 2000, 0.01), c.create(paddedInput, 2000, 0.01))
    }

    it should "support SCollection syntax" in {
      runWithContext { sc =>
        implicit val coder = c.coder
        val coll = sc.parallelize(paddedInput)
        coll.asApproxFilter(c) should satisfySingleValue[c.Filter[Int]] {
          eq(_, c.create(paddedInput))
        }
        coll.asApproxFilter(c, 2000) should satisfySingleValue[c.Filter[Int]] {
          eq(_, c.create(paddedInput, 2000))
        }
        coll.asApproxFilter(c, 2000, 0.01) should satisfySingleValue[c.Filter[Int]] {
          eq(_, c.create(paddedInput, 2000, 0.01))
        }
      }
    }

    it should "support Java serialization" in {
      val orig = c.create(paddedInput, 2000, 0.01)
      val copy = SerializableUtils.clone(orig)
      copy.approxElementCount shouldBe orig.approxElementCount
      copy.expectedFpp shouldBe orig.expectedFpp
      (1 to 2000).map(copy.mightContain) shouldBe (1 to 2000).map(orig.mightContain)
    }

    it should "support Coder serialization" in {
      val coder = CoderMaterializer.beamWithDefault(c.coder)
      val orig = c.create(paddedInput, 2000, 0.01)
      val copy = CoderUtils.clone(coder, orig)
      copy.approxElementCount shouldBe orig.approxElementCount
      copy.expectedFpp shouldBe orig.expectedFpp
      (1 to 2000).map(copy.mightContain) shouldBe (1 to 2000).map(orig.mightContain)
    }

    def eq(lhs: c.Filter[Int], rhs: c.Filter[Int]): Boolean =
      lhs.approxElementCount == rhs.approxElementCount &&
        lhs.expectedFpp == rhs.expectedFpp &&
        (1 to 2000).map(lhs.mightContain) == (1 to 2000).map(rhs.mightContain)
  }

  test(BloomFilter)
}
