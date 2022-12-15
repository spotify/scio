/*
 * Copyright 2020 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.hash

import com.spotify.scio.testing.PipelineSpec
import magnolify.guava.auto._

import scala.util.Random

class MutableScalableBloomFilterTest extends PipelineSpec {
  def compoundedErrorRate(fpProb: Double, tighteningRatio: Double, numFilters: Int): Double =
    1 - (0 to numFilters).foldLeft(1.0)((e, i) =>
      e * 1 - fpProb * math.pow(tighteningRatio, i.toDouble)
    )

  "A MutableScalableBloomFilter" should "not grow for repeated items" in {
    val sbf = MutableScalableBloomFilter[String](256, 0.01)
    assert(sbf.numFilters == 0)
    assert(sbf.approximateElementCount == 0)

    (0 to 100).foreach(_ => sbf += "test")
    assert(sbf.mightContain("test"))
    assert(sbf.numFilters == 1)
    assert(sbf.approximateElementCount == 1)
  }

  it should "converge below the compounded false positive probability rate" in {
    val fpProb = 0.001
    val tr = 0.5
    val sbf = MutableScalableBloomFilter[String](64, fpProb, 4, tr)
    val inserts = 500
    val trials = 100000

    // insert a bunch of random 16 character strings
    val random = new Random(42)
    (0 until inserts).foreach(_ => sbf += random.nextString(16))

    // check for the presense of any 8 character strings
    val fpCount = (0 until trials).count(_ => sbf.mightContain(random.nextString(8))).toDouble
    assert(fpCount / trials <= compoundedErrorRate(fpProb, tr, sbf.numFilters))
  }

  it should "grow at the given growth rate" in {
    val initialCapacity = 2L
    val sbf = MutableScalableBloomFilter[String](initialCapacity, 0.001, 2, 1.0)
    assert(sbf.numFilters == 0)

    (0 until 100).foreach(i => sbf += ("item" + i))
    assert(sbf.numFilters == 6) // filter sizes: 2 + 4 + 8 + 16 + 32 + 64 = 126 > 100

    val sbf2 = MutableScalableBloomFilter[String](initialCapacity, 0.001, 4, 1.0)
    assert(sbf2.numFilters == 0)

    (0 until 100).foreach(i => sbf2 += ("item" + i))
    assert(sbf2.numFilters == 4) // filter sizes: 2 + 8 + 64 + 512 > 100
  }

  it should "work in an SCollection" in {
    runWithContext { sc =>
      val funnel = implicitly[com.google.common.hash.Funnel[String]]
      val inVals = List("foo", "bar", "baz")

      val out = sc
        .parallelize(inVals)
        .groupBy(_ => ())
        .map { case (_, strs) =>
          MutableScalableBloomFilter[String](10)(funnel) ++= strs
        }
      out should satisfySingleValue { sbf: MutableScalableBloomFilter[String] =>
        inVals.foldLeft(true) { case (st, v) => st && sbf.mightContain(v) }
      }
    }
  }

  it should "round-trip serialization" in {
    val initialCapacity = 1000L
    val sbf = MutableScalableBloomFilter[String](initialCapacity)
    (0 until 100).foreach(i => sbf += ("item" + i))

    val roundtripped =
      MutableScalableBloomFilter.fromBytes[String](MutableScalableBloomFilter.toBytes(sbf))
    roundtripped.deserialize()
    assert(roundtripped == sbf)

    // add some new things
    (0 until 1000).foreach(i => roundtripped += "foo" + i)
    val r2 =
      MutableScalableBloomFilter.fromBytes[String](MutableScalableBloomFilter.toBytes(roundtripped))
    r2.deserialize()
    assert(r2 == roundtripped)

    // add same things again
    (0 until 10000).foreach(i => r2 += "foo" + i)
    // add some new things
    (0 until 10000).foreach(i => r2 += "baz" + i)
    val r3 = MutableScalableBloomFilter.fromBytes[String](MutableScalableBloomFilter.toBytes(r2))
    r3.deserialize()
    assert(r3 == r2)
  }

  it should "require positive initialCapacity" in {
    assertThrows[IllegalArgumentException] {
      MutableScalableBloomFilter[String](0)
    }
  }
}
