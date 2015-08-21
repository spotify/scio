/* Ported from org.apache.spark.rdd.DoubleRDDSuite */

package com.spotify.scio.values

import com.spotify.scio.testing.PipelineTest

class HistogramTest extends PipelineTest {

  "DoubleSCollectionFunctions.histogram" should "work on empty input" in {
    runWithContext { sc =>
      val p = sc.parallelize[Double]()
      p.histogram(Array(0.0, 10.0)).internal should containSingleValue (Array(0L))
    }
  }

  it should "work out of range with one bucket" in {
    runWithContext { sc =>
      val p = sc.parallelize(10.01, -0.01)
      p.histogram(Array(0.0, 10.0)).internal should containSingleValue (Array(0L))
    }
  }

  it should "work in range with one bucket" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 2.0, 3.0, 4.0)
      p.histogram(Array(0.0, 10.0)).internal should containSingleValue (Array(4L))
    }
  }

  it should "work in range with one bucket exact match" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 2.0, 3.0, 4.0)
      p.histogram(Array(1.0, 4.0)).internal should containSingleValue (Array(4L))
    }
  }

  it should "work out of range with two buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(10.01, -0.01)
      p.histogram(Array(0.0, 5.0, 10.0)).internal should containSingleValue (Array(0L, 0L))
    }
  }

  it should "work out of range with two uneven buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(10.01, -0.01)
      p.histogram(Array(0.0, 4.0, 10.0)).internal should containSingleValue (Array(0L, 0L))
    }
  }

  it should "work in range with two buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 2.0, 3.0, 5.0, 6.0)
      p.histogram(Array(0.0, 5.0, 10.0)).internal should containSingleValue (Array(3L, 2L))
    }
  }

  it should "work in range with two buckets and NaN" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 2.0, 3.0, 5.0, 6.0, Double.NaN)
      p.histogram(Array(0.0, 5.0, 10.0)).internal should containSingleValue (Array(3L, 2L))
    }
  }

  it should "work in range with two uneven buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 2.0, 3.0, 5.0, 6.0)
      p.histogram(Array(0.0, 5.0, 11.0)).internal should containSingleValue (Array(3L, 2L))
    }
  }

  it should "work mixed range with two uneven buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(-0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.0, 11.01)
      p.histogram(Array(0.0, 5.0, 11.0)).internal should containSingleValue (Array(4L, 3L))
    }
  }

  it should "work mixed range with four uneven buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(-0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.1, 12.0, 199.0, 200.0, 200.1)
      p.histogram(Array(0.0, 5.0, 11.0, 12.0, 200.0)).internal should containSingleValue (Array(4L, 2L, 1L, 3L))
    }
  }

  it should "work mixed range with four uneven buckets and NaN" in {
    runWithContext { sc =>
      val p = sc.parallelize(-0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.1, 12.0, 199.0, 200.0, 200.1, Double.NaN)
      p.histogram(Array(0.0, 5.0, 11.0, 12.0, 200.0)).internal should containSingleValue (Array(4L, 2L, 1L, 3L))
    }
  }

  it should "work mixed range with four uneven buckets, NaN and NaN range" in {
    runWithContext { sc =>
      val p = sc.parallelize(-0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.1, 12.0, 199.0, 200.0, 200.1, Double.NaN)
      val buckets = Array(0.0, 5.0, 11.0, 12.0, 200.0, Double.NaN)
      p.histogram(buckets).internal should containSingleValue (Array(4L, 2L, 1L, 2L, 3L))
    }
  }

  it should "work mixed range with four uneven buckets, NaN, NaN range and infinity" in {
    runWithContext { sc =>
      val p = sc.parallelize(
        -0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.1, 12.0, 199.0, 200.0, 200.1,
        Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN)
      val buckets = Array(0.0, 5.0, 11.0, 12.0, 200.0, Double.NaN)
      p.histogram(buckets).internal should containSingleValue (Array(4L, 2L, 1L, 2L, 4L))
    }
  }

  it should "work out of range with infinite buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(10.01, -0.01, Double.NaN)
      val buckets = Array(Double.NegativeInfinity, 0.0, Double.PositiveInfinity)
      p.histogram(buckets).internal should containSingleValue (Array(1L, 1L))
    }
  }

  it should "throw exception on invalid bucket array" in {
    intercept[RuntimeException] {
      runWithContext { sc =>
        val p = sc.parallelize(1.0)
        p.histogram(Array.empty[Double])
      }
    }

    intercept[RuntimeException] {
      runWithContext { sc =>
        val p = sc.parallelize(1.0)
        p.histogram(Array(1.0))
      }
    }
  }

  it should "work without buckets, basic" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 2.0, 3.0, 4.0)
      val (buckets, histogram) = p.histogram(1)
      buckets.internal should containSingleValue (Array(1.0, 4.0))
      histogram.internal should containSingleValue (Array(4L))
    }
  }

  it should "work without buckets, basic, single element" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0)
      val (buckets, histogram) = p.histogram(1)
      buckets.internal should containSingleValue (Array(1.0, 1.0))
      histogram.internal should containSingleValue (Array(1L))
    }
  }

  it should "work without buckets, basic, no range" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 1.0, 1.0, 1.0)
      val (buckets, histogram) = p.histogram(1)
      buckets.internal should containSingleValue (Array(1.0, 1.0))
      histogram.internal should containSingleValue (Array(4L))
    }
  }

  it should "work without buckets, basic, two" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 2.0, 3.0, 4.0)
      val (buckets, histogram) = p.histogram(2)
      buckets.internal should containSingleValue (Array(1.0, 2.5, 4.0))
      histogram.internal should containSingleValue (Array(2L, 2L))
    }
  }

  it should "work with double values at min/max" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 1.0, 1.0, 2.0, 3.0, 3.0)
      p.histogram(4)._2.internal should containSingleValue (Array(3L, 0L, 1L, 2L))
      p.histogram(3)._2.internal should containSingleValue (Array(3L, 1L, 2L))
    }
  }

  it should "work without buckets and more requested than elements" in {
    runWithContext { sc =>
      val p = sc.parallelize(1.0, 2.0)
      val (buckets, histogram) = p.histogram(10)
      buckets.internal should containSingleValue (Array(1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0))
      histogram.internal should containSingleValue (Array(1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L))
    }
  }

  it should "work without buckets for larger datasets" in {
    runWithContext { sc =>
      val p = sc.parallelize((6 to 99).map(_.toDouble): _*)
      val (buckets, histogram) = p.histogram(8)
      buckets.internal should containSingleValue (Array(6.0, 17.625, 29.25, 40.875, 52.5, 64.125, 75.75, 87.375, 99.0))
      histogram.internal should containSingleValue (Array(12L, 12L, 11L, 12L, 12L, 11L, 12L, 12L))
    }
  }

  it should "work without buckets and non-integral bucket edges" in {
    runWithContext { sc =>
      val p = sc.parallelize((6 to 99).map(_.toDouble): _*)
      val (buckets, histogram) = p.histogram(9)
      buckets.map(_.head).internal should containSingleValue (6.0)
      buckets.map(_.last).internal should containSingleValue (99.0)
      histogram.internal should containSingleValue (Array(11L, 10L, 10L, 11L, 10L, 10L, 11L, 10L, 11L))
    }
  }

  it should "work with huge range" in {
    runWithContext { sc =>
      val p = sc.parallelize(0.0, 1.0e24, 1.0e30)
      val histogram = p.histogram(1000000)._2
      histogram.map(_(0)).internal should containSingleValue (1L)
      histogram.map(_(1)).internal should containSingleValue (1L)
      histogram.map(_.last).internal should containSingleValue (1L)
      histogram.map(h => (2 to h.length - 2).forall(i => h(i) == 0)).internal should containSingleValue (true)
    }
  }

  it should "throw exception on invalid SCollections" in {
    intercept[RuntimeException] {
      runWithContext { sc =>
        sc.parallelize(1.0, Double.PositiveInfinity).histogram(1)
      }
    }

    intercept[RuntimeException] {
      runWithContext { sc =>
        sc.parallelize(1.0, Double.NaN).histogram(1)
      }
    }

    intercept[RuntimeException] {
      runWithContext { sc =>
        sc.parallelize[Double]().histogram(1)
      }
    }
  }

}
