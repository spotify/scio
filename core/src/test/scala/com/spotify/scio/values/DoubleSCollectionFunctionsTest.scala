package com.spotify.scio.values

import com.spotify.scio.testing.PipelineTest
import com.spotify.scio.util.StatCounter

class DoubleSCollectionFunctionsTest extends PipelineTest {

  val ints = 1 to 100
  val longs = (1 to 100).map(_.toLong)
  val floats = (1 to 100).map(_.toFloat)
  val doubles = (1 to 100).map(_.toDouble)
  val expected = StatCounter((1 to 100).map(_.toDouble): _*)

  def test(s: Seq[Double], e: Double): Unit = {
    s.size should equal (1L)
    s.head should equal (e +- 1e-10)
  }

  "DoubleSCollection" should "support sampleStdev()" in {
    val e = expected.sampleStdev
    test(runWithData(ints: _*)(_.sampleStdev()), e)
    test(runWithData(longs: _*)(_.sampleStdev()), e)
    test(runWithData(floats: _*)(_.sampleStdev()), e)
    test(runWithData(doubles: _*)(_.sampleStdev()), e)
  }

  it should "support sampleVariance()" in {
    val e = expected.sampleVariance
    test(runWithData(ints: _*)(_.sampleVariance()), e)
    test(runWithData(longs: _*)(_.sampleVariance()), e)
    test(runWithData(floats: _*)(_.sampleVariance()), e)
    test(runWithData(doubles: _*)(_.sampleVariance()), e)
  }

  it should "support stdev()" in {
    val e = expected.stdev
    test(runWithData(ints: _*)(_.stdev()), e)
    test(runWithData(longs: _*)(_.stdev()), e)
    test(runWithData(floats: _*)(_.stdev()), e)
    test(runWithData(doubles: _*)(_.stdev()), e)
  }

  it should "support variance()" in {
    val e = expected.variance
    test(runWithData(ints: _*)(_.variance()), e)
    test(runWithData(longs: _*)(_.variance()), e)
    test(runWithData(floats: _*)(_.variance()), e)
    test(runWithData(doubles: _*)(_.variance()), e)
  }

}
