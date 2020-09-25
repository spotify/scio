package com.spotify.scio.hash

import com.spotify.scio.testing.PipelineSpec
import magnolify.guava.auto._

import scala.util.Random

class MutableScalableBloomFilterTest extends PipelineSpec {
  def compoundedErrorRate(fpProb: Double, tighteningRatio: Double, numFilters: Int): Double =
    1 - (0 to numFilters).foldLeft(1.0)(_ * 1 - fpProb * math.pow(tighteningRatio, _))

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
    val initialCapacity = 2
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
    val initialCapacity = 2
    val sbf = MutableScalableBloomFilter[String](initialCapacity, 0.001, 2, 1.0)
    (0 until 100).foreach(i => sbf += ("item" + i))
    val roundtripped = MutableScalableBloomFilter.fromBytes[String](MutableScalableBloomFilter.toBytes(sbf))
    roundtripped.deserialize()
    assert(roundtripped == sbf)
  }
}
