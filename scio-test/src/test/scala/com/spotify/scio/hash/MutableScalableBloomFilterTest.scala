package com.spotify.scio.hash

import magnolify.guava.auto._
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Random

class MutableScalableBloomFilterTest extends AnyFlatSpec {
  def compoundedErrorRate(fpProb: Double, tighteningRatio: Double, numFilters: Int): Double =
    1 - (0 to numFilters).foldLeft(1.0)(_ * 1 - fpProb * math.pow(tighteningRatio, _))

  "A MutableScalableBloomFilter" should "not grow for repeated items" in {
    val sbf = MutableScalableBloomFilter[String](256, 0.01)
    assert(sbf.filters.isEmpty)
    assert(sbf.approximateElementCount == 0)

    (0 to 100).foreach(_ => sbf + "test")
    assert(sbf.contains("test"))
    assert(sbf.filters.size == 1)
    assert(sbf.approximateElementCount == 1)
  }

  it should "converge below the compounded false probability rate" in {
    val fpProb = 0.001
    val tr = 0.5
    val sbf = MutableScalableBloomFilter[String](
      initialCapacity = 64,
      fpProb = fpProb,
      growthRate = 4,
      tighteningRatio = tr
    )
    val inserts = 500
    val trials = 100000

    // insert a bunch of random 16 character strings
    val random = new Random(42)
    (0 until inserts).foreach(_ => sbf + random.nextString(16))

    // check for the presense of any 8 character strings
    val fpCount = (0 until trials).count(_ => sbf.contains(random.nextString(8))).toDouble
    assert(fpCount / trials <= compoundedErrorRate(fpProb, tr, sbf.filters.size))
  }

  it should "grow at the given growth rate" in {
    val initialCapacity = 2
    val sbf = MutableScalableBloomFilter[String](initialCapacity, 0.001, growthRate = 2, 1.0)
    assert(sbf.filters.isEmpty)

    (0 until 100).foreach(i => sbf + ("item" + i))
    assert(sbf.filters.size == 6) // filter sizes: 2 + 4 + 8 + 16 + 32 + 64 = 126 > 100

    val sbf2 = MutableScalableBloomFilter[String](initialCapacity, 0.001, growthRate = 4, 1.0)
    assert(sbf2.filters.isEmpty)

    (0 until 100).foreach(i => sbf2 + ("item" + i))
    assert(sbf2.filters.size == 4) // filter sizes: 2 + 8 + 64 + 512 > 100
  }

  it should "provide size as the sum of underlying sizes" in {
    val sbf =
      MutableScalableBloomFilter[String](128, 0.00001, growthRate = 4, tighteningRatio = 0.9)
    val random = new Random(42)
    (0 to 1000).foreach(_ => sbf + random.nextString(8))
    val actual = sbf.approximateElementCount
    val expected = sbf.filters.foldLeft(0L)(_ + _.approximateElementCount)
    assert(actual == expected)
  }
}
