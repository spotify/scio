package com.spotify.cloud.dataflow.values

import com.spotify.cloud.dataflow.testing.PipelineTest
import com.spotify.cloud.dataflow.util.StatCounter

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
    runWithContext { pipeline =>
      val e = expected.sampleStdev
      val p1 = pipeline.parallelize(ints: _*).sampleStdev()
      val p2 = pipeline.parallelize(longs: _*).sampleStdev()
      val p3 = pipeline.parallelize(floats: _*).sampleStdev()
      val p4 = pipeline.parallelize(doubles: _*).sampleStdev()
      checkError(p1, e)
      checkError(p2, e)
      checkError(p3, e)
      checkError(p4, e)
    }
  }

  it should "support sampleVariance()" in {
    runWithContext { pipeline =>
      val e = expected.sampleVariance
      val p1 = pipeline.parallelize(ints: _*).sampleVariance()
      val p2 = pipeline.parallelize(longs: _*).sampleVariance()
      val p3 = pipeline.parallelize(floats: _*).sampleVariance()
      val p4 = pipeline.parallelize(doubles: _*).sampleVariance()
      checkError(p1, e)
      checkError(p2, e)
      checkError(p3, e)
      checkError(p4, e)
    }
  }

  it should "support stdev()" in {
    runWithContext { pipeline =>
      val e = expected.stdev
      val p1 = pipeline.parallelize(ints: _*).stdev()
      val p2 = pipeline.parallelize(longs: _*).stdev()
      val p3 = pipeline.parallelize(floats: _*).stdev()
      val p4 = pipeline.parallelize(doubles: _*).stdev()
      checkError(p1, e)
      checkError(p2, e)
      checkError(p3, e)
      checkError(p4, e)
    }
  }

  it should "support variance()" in {
    runWithContext { pipeline =>
      val e = expected.variance
      val p1 = pipeline.parallelize(ints: _*).variance()
      val p2 = pipeline.parallelize(longs: _*).variance()
      val p3 = pipeline.parallelize(floats: _*).variance()
      val p4 = pipeline.parallelize(doubles: _*).variance()
      checkError(p1, e)
      checkError(p2, e)
      checkError(p3, e)
      checkError(p4, e)
    }
  }

}
