/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.util.random

import java.util.Random

import org.apache.commons.math3.distribution.PoissonDistribution

// Ported from org.apache.spark.util.random.RandomSamplerSuite
object RandomSamplerUtils extends Serializable {

  val populationSize = 10000

  /**
   * My statistical testing methodology is to run a Kolmogorov-Smirnov (KS) test
   * between the random samplers and simple reference samplers (known to work correctly).
   * The sampling gap sizes between chosen samples should show up as having the same
   * distributions between test and reference, if things are working properly.  That is,
   * the KS test will fail to strongly reject the null hypothesis that the distributions of
   * sampling gaps are the same.
   * There are no actual KS tests implemented for scala (that I can find) - and so what I
   * have done here is pre-compute "D" - the KS statistic - that corresponds to a "weak"
   * p-value for a particular sample size.  I can then test that my measured KS stats
   * are less than D.  Computing D-values is easy, and implemented below.
   *
   * I used the scipy 'kstwobign' distribution to pre-compute my D value:
   *
   * def ksdval(q=0.1, n=1000):
   *     en = np.sqrt(float(n) / 2.0)
   *     return stats.kstwobign.isf(float(q)) / (en + 0.12 + 0.11 / en)
   *
   * When comparing KS stats I take the median of a small number of independent test runs
   * to compensate for the issue that any sampled statistic will show "false positive" with
   * some probability.  Even when two distributions are the same, they will register as
   * different 10% of the time at a p-value of 0.1
   */

  // This D value is the precomputed KS statistic for p-value 0.1, sample size 1000:
  val sampleSize = 1000
  val D = 0.0544280747619

  val fixedSeed = 235711L

  // I'm not a big fan of fixing seeds, but unit testing based on running statistical tests
  // will always fail with some nonzero probability, so I'll fix the seed to prevent these
  // tests from generating random failure noise in CI testing, etc.
  val rngSeed: Random = RandomSampler.newDefaultRNG
  rngSeed.setSeed(fixedSeed)

  // Reference implementation of sampling without replacement (bernoulli)
  def sample[T](data: Iterator[T], f: Double): Iterator[T] = {
    val rng: Random = RandomSampler.newDefaultRNG
    rng.setSeed(rngSeed.nextLong)
    data.filter(_ => rng.nextDouble <= f)
  }

  // Reference implementation of sampling with replacement
  def sampleWR[T](data: Iterator[T], f: Double): Iterator[T] = {
    val rng = new PoissonDistribution(f)
    rng.reseedRandomGenerator(rngSeed.nextLong)
    data.flatMap { v =>
      val rep = rng.sample()
      if (rep == 0) Iterator.empty else Iterator.fill(rep)(v)
    }
  }

  // Returns iterator over gap lengths between samples.
  // This function assumes input data is integers sampled from the sequence of
  // increasing integers: {0, 1, 2, ...}.  This works because that is how I generate them,
  // and the samplers preserve their input order
  def gaps(data: Array[Int]): Array[Int] = data.sliding(2).map(x => x(1) - x(0)).toArray

  // Returns the cumulative distribution from a histogram
  def cumulativeDist(hist: Array[Int]): Array[Double] = {
    val n = hist.sum.toDouble
    assert(n > 0.0)
    hist.scanLeft(0)(_ + _).drop(1).map { _.toDouble / n }
  }

  // Returns aligned cumulative distributions from two arrays of data
  def cumulants(d1: Array[Int], d2: Array[Int],
                ss: Int = sampleSize): (Array[Double], Array[Double]) = {
    assert(math.min(d1.length, d2.length) > 0)
    assert(math.min(d1.min, d2.min)  >=  0)
    val m = 1 + math.max(d1.max, d2.max)
    val h1 = Array.fill[Int](m)(0)
    val h2 = Array.fill[Int](m)(0)
    for (v <- d1) { h1(v) += 1 }
    for (v <- d2) { h2(v) += 1 }
    assert(h1.sum == h2.sum)
    assert(h1.sum == ss)
    (cumulativeDist(h1), cumulativeDist(h2))
  }

  // Computes the Kolmogorov-Smirnov 'D' statistic from two cumulative distributions
  def ksd(cdf1: Array[Double], cdf2: Array[Double]): Double = {
    assert(cdf1.length == cdf2.length)
    val n = cdf1.length
    assert(n > 0)
    assert(cdf1(n-1) == 1.0)
    assert(cdf2(n-1) == 1.0)
    cdf1.zip(cdf2).map { x => Math.abs(x._1 - x._2) }.max
  }

  // Returns the median KS 'D' statistic between two samples, over (m) sampling trials
  def medianKSD(data1: => Array[Int], data2: => Array[Int], m: Int = 5): Double = {
    val t = Array.fill[Double](m) {
      val (c1, c2) = cumulants(data1.take(sampleSize), data2.take(sampleSize))
      ksd(c1, c2)
    }.sorted
    // return the median KS statistic
    t(m / 2)
  }

  val population = 1 to populationSize
  val keyedPopulation = population.map(("a", _)) ++ population.map(("b", _))
  def expectedSamples(withReplacement: Boolean, fraction: Double): Array[Int] = {
    val i = population.iterator
    val s = if (withReplacement) sampleWR(i, fraction) else sample(i, fraction)
    s.toArray
  }

}
