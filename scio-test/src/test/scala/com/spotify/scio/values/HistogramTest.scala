// scalastyle:off header.matches
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:on header.matches
/* Ported from org.apache.spark.rdd.DoubleRDDSuite */

package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec

class HistogramTest extends PipelineSpec {

  "DoubleSCollectionFunctions.histogram" should "work on empty input" in {
    runWithContext { sc =>
      val p = sc.parallelize[Double](Seq.empty)
      p.histogram(Array(0.0, 10.0)) should containSingleValue (Array(0L))
    }
  }

  it should "work out of range with one bucket" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(10.01, -0.01))
      p.histogram(Array(0.0, 10.0)) should containSingleValue (Array(0L))
    }
  }

  it should "work in range with one bucket" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0))
      p.histogram(Array(0.0, 10.0)) should containSingleValue (Array(4L))
    }
  }

  it should "work in range with one bucket exact match" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0))
      p.histogram(Array(1.0, 4.0)) should containSingleValue (Array(4L))
    }
  }

  it should "work out of range with two buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(10.01, -0.01))
      p.histogram(Array(0.0, 5.0, 10.0)) should containSingleValue (Array(0L, 0L))
    }
  }

  it should "work out of range with two uneven buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(10.01, -0.01))
      p.histogram(Array(0.0, 4.0, 10.0)) should containSingleValue (Array(0L, 0L))
    }
  }

  it should "work in range with two buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0, 3.0, 5.0, 6.0))
      p.histogram(Array(0.0, 5.0, 10.0)) should containSingleValue (Array(3L, 2L))
    }
  }

  it should "work in range with two buckets and NaN" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0, 3.0, 5.0, 6.0, Double.NaN))
      p.histogram(Array(0.0, 5.0, 10.0)) should containSingleValue (Array(3L, 2L))
    }
  }

  it should "work in range with two uneven buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0, 3.0, 5.0, 6.0))
      p.histogram(Array(0.0, 5.0, 11.0)) should containSingleValue (Array(3L, 2L))
    }
  }

  it should "work mixed range with two uneven buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(-0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.0, 11.01))
      p.histogram(Array(0.0, 5.0, 11.0)) should containSingleValue (Array(4L, 3L))
    }
  }

  it should "work mixed range with four uneven buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(
        Seq(-0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.1, 12.0, 199.0, 200.0, 200.1))
      p.histogram(Array(0.0, 5.0, 11.0, 12.0, 200.0)) should
        containSingleValue (Array(4L, 2L, 1L, 3L))
    }
  }

  it should "work mixed range with four uneven buckets and NaN" in {
    runWithContext { sc =>
      val p = sc.parallelize(
        Seq(-0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.1, 12.0, 199.0, 200.0, 200.1, Double.NaN))
      p.histogram(Array(0.0, 5.0, 11.0, 12.0, 200.0)) should
        containSingleValue (Array(4L, 2L, 1L, 3L))
    }
  }

  it should "work mixed range with four uneven buckets, NaN and NaN range" in {
    runWithContext { sc =>
      val p = sc.parallelize(
        Seq(-0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.1, 12.0, 199.0, 200.0, 200.1, Double.NaN))
      val buckets = Array(0.0, 5.0, 11.0, 12.0, 200.0, Double.NaN)
      p.histogram(buckets) should containSingleValue (Array(4L, 2L, 1L, 2L, 3L))
    }
  }

  it should "work mixed range with four uneven buckets, NaN, NaN range and infinity" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(
        -0.01, 0.0, 1.0, 2.0, 3.0, 5.0, 6.0, 11.1, 12.0, 199.0, 200.0, 200.1,
        Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN))
      val buckets = Array(0.0, 5.0, 11.0, 12.0, 200.0, Double.NaN)
      p.histogram(buckets) should containSingleValue (Array(4L, 2L, 1L, 2L, 4L))
    }
  }

  it should "work out of range with infinite buckets" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(10.01, -0.01, Double.NaN))
      val buckets = Array(Double.NegativeInfinity, 0.0, Double.PositiveInfinity)
      p.histogram(buckets) should containSingleValue (Array(1L, 1L))
    }
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail on invalid bucket array" in {
    val msg = "java.lang.IllegalArgumentException: requirement failed: " +
      "buckets array must have at least two elements"
    the [RuntimeException] thrownBy {
      runWithContext { _.parallelize(Seq(1.0)).histogram(Array.empty[Double]) }
    } should have message msg

    the [RuntimeException] thrownBy {
      runWithContext { _.parallelize(Seq(1.0)).histogram(Array(1.0)) }
    } should have message msg
  }
  // scalastyle:on no.whitespace.before.left.bracket

  it should "work without buckets, basic" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0))
      val (buckets, histogram) = p.histogram(1)
      buckets should containSingleValue (Array(1.0, 4.0))
      histogram should containSingleValue (Array(4L))
    }
  }

  it should "work without buckets, basic, single element" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0))
      val (buckets, histogram) = p.histogram(1)
      buckets should containSingleValue (Array(1.0, 1.0))
      histogram should containSingleValue (Array(1L))
    }
  }

  it should "work without buckets, basic, no range" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 1.0, 1.0, 1.0))
      val (buckets, histogram) = p.histogram(1)
      buckets should containSingleValue (Array(1.0, 1.0))
      histogram should containSingleValue (Array(4L))
    }
  }

  it should "work without buckets, basic, two" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0))
      val (buckets, histogram) = p.histogram(2)
      buckets should containSingleValue (Array(1.0, 2.5, 4.0))
      histogram should containSingleValue (Array(2L, 2L))
    }
  }

  it should "work with double values at min/max" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 1.0, 1.0, 2.0, 3.0, 3.0))
      p.histogram(4)._2 should containSingleValue (Array(3L, 0L, 1L, 2L))
      p.histogram(3)._2 should containSingleValue (Array(3L, 1L, 2L))
    }
  }

  it should "work without buckets and more requested than elements" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0))
      val (buckets, histogram) = p.histogram(10)
      buckets should
        containSingleValue (Array(1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0))
      histogram should containSingleValue (Array(1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L))
    }
  }

  it should "work without buckets for larger datasets" in {
    runWithContext { sc =>
      val p = sc.parallelize((6 to 99).map(_.toDouble))
      val (buckets, histogram) = p.histogram(8)
      buckets should
        containSingleValue (Array(6.0, 17.625, 29.25, 40.875, 52.5, 64.125, 75.75, 87.375, 99.0))
      histogram should containSingleValue (Array(12L, 12L, 11L, 12L, 12L, 11L, 12L, 12L))
    }
  }

  it should "work without buckets and non-integral bucket edges" in {
    runWithContext { sc =>
      val p = sc.parallelize((6 to 99).map(_.toDouble))
      val (buckets, histogram) = p.histogram(9)
      buckets.map(_.head) should containSingleValue (6.0)
      buckets.map(_.last) should containSingleValue (99.0)
      histogram should containSingleValue (Array(11L, 10L, 10L, 11L, 10L, 10L, 11L, 10L, 11L))
    }
  }

  it should "work with huge range" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(0.0, 1.0e24, 1.0e30))
      val histogram = p.histogram(1000000)._2
      histogram.map(_(0)) should containSingleValue (1L)
      histogram.map(_(1)) should containSingleValue (1L)
      histogram.map(_.last) should containSingleValue (1L)
      histogram.map(h => (2 to h.length - 2).forall(i => h(i) == 0)) should
        containSingleValue (true)
    }
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail on invalid SCollections" in {
    val msg = "java.lang.UnsupportedOperationException: " +
      "Histogram on either an empty SCollection or SCollection containing +/-infinity or NaN"
    the [RuntimeException] thrownBy {
      runWithContext { _.parallelize(Seq(1.0, Double.PositiveInfinity)).histogram(1) }
    } should have message msg

    the [RuntimeException] thrownBy {
      runWithContext { _.parallelize(Seq(1.0, Double.NaN)).histogram(1) }
    } should have message msg

    the [RuntimeException] thrownBy {
      runWithContext { _.parallelize[Double](Seq.empty).histogram(1) }
    } should have message
      "java.util.NoSuchElementException: Empty PCollection accessed as a singleton view."
  }
  // scalastyle:on no.whitespace.before.left.bracket

}
