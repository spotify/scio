package com.spotify.scio.values

import com.spotify.scio.testing.PipelineTest
import com.spotify.scio.util.StatCounter

class DoubleSCollectionFunctionsTest extends PipelineTest {

  val ints = 1 to 100
  val longs = (1 to 100).map(_.toLong)
  val floats = (1 to 100).map(_.toFloat)
  val doubles = (1 to 100).map(_.toDouble)

  val expected = StatCounter((1 to 100).map(_.toDouble): _*)

  def checkError(p: SCollection[Double], e: Double): Unit = {
    p.count().internal should containSingleValue (1L)
    p.map(x => math.abs(x - e) < 1e-10).internal should containSingleValue (true)
  }

  "DoubleSCollection" should "support sampleStdev()" in {
    runWithContext { sc =>
      val e = expected.sampleStdev
      val p1 = sc.parallelize(ints: _*).sampleStdev()
      val p2 = sc.parallelize(longs: _*).sampleStdev()
      val p3 = sc.parallelize(floats: _*).sampleStdev()
      val p4 = sc.parallelize(doubles: _*).sampleStdev()
      checkError(p1, e)
      checkError(p2, e)
      checkError(p3, e)
      checkError(p4, e)
    }
  }

  it should "support sampleVariance()" in {
    runWithContext { sc =>
      val e = expected.sampleVariance
      val p1 = sc.parallelize(ints: _*).sampleVariance()
      val p2 = sc.parallelize(longs: _*).sampleVariance()
      val p3 = sc.parallelize(floats: _*).sampleVariance()
      val p4 = sc.parallelize(doubles: _*).sampleVariance()
      checkError(p1, e)
      checkError(p2, e)
      checkError(p3, e)
      checkError(p4, e)
    }
  }

  it should "support stdev()" in {
    runWithContext { sc =>
      val e = expected.stdev
      val p1 = sc.parallelize(ints: _*).stdev()
      val p2 = sc.parallelize(longs: _*).stdev()
      val p3 = sc.parallelize(floats: _*).stdev()
      val p4 = sc.parallelize(doubles: _*).stdev()
      checkError(p1, e)
      checkError(p2, e)
      checkError(p3, e)
      checkError(p4, e)
    }
  }

  it should "support variance()" in {
    runWithContext { sc =>
      val e = expected.variance
      val p1 = sc.parallelize(ints: _*).variance()
      val p2 = sc.parallelize(longs: _*).variance()
      val p3 = sc.parallelize(floats: _*).variance()
      val p4 = sc.parallelize(doubles: _*).variance()
      checkError(p1, e)
      checkError(p2, e)
      checkError(p3, e)
      checkError(p4, e)
    }
  }

}
