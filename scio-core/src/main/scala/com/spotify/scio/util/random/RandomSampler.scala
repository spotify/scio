// scalastyle:off header.matches
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:on header.matches

/* Ported from org.apache.spark.util.random.RandomSampler */

package com.spotify.scio.util.random

import java.util.{Random => JRandom}

import org.apache.beam.sdk.transforms.DoFn
import org.apache.commons.math3.distribution.{IntegerDistribution, PoissonDistribution}

private[scio] object RandomSampler {
  /** Default random number generator used by random samplers. */
  def newDefaultRNG: JRandom = new XORShiftRandom

  /**
   * Sampling fraction arguments may be results of computation, and subject to floating
   * point jitter.  I check the arguments with this epsilon slop factor to prevent spurious
   * warnings for cases such as summing some numbers to get a sampling fraction of 1.000000001
   */
  val roundingEpsilon = 1e-6
}

private[scio] abstract class RandomSampler[T, R] extends DoFn[T, T] {

  protected var rng: R = null.asInstanceOf[R]
  protected var seed: Long = -1

  // TODO: is it necessary to setSeed for each instance like Spark does?
  override def startBundle(c: DoFn[T, T]#Context): Unit = { rng = init }

  override def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
    val element = c.element()
    val count = samples
    var i = 0
    while (i < count) {
      c.output(element)
      i += 1
    }
  }

  def init: R
  def samples: Int
  def setSeed(seed: Long): Unit = this.seed = seed

}

/**
 * A sampler based on Bernoulli trials.
 *
 * @param fraction the sampling fraction, aka Bernoulli sampling probability
 * @tparam T item type
 */
private[scio] class BernoulliSampler[T](fraction: Double) extends RandomSampler[T, JRandom] {

  /** Epsilon slop to avoid failure from floating point jitter */
  require(
    fraction >= (0.0 - RandomSampler.roundingEpsilon)
      && fraction <= (1.0 + RandomSampler.roundingEpsilon),
    s"Sampling fraction ($fraction) must be on interval [0, 1]")

  override def init: JRandom = {
    val r = RandomSampler.newDefaultRNG
    if (seed > 0) {
      r.setSeed(seed)
    }
    r
  }

  override def samples: Int = {
    if (fraction <= 0.0) {
      0
    } else if (fraction >= 1.0) {
      1
    } else {
      if (rng.nextDouble() <= fraction) 1 else 0
    }
  }

}

/**
 * A sampler for sampling with replacement, based on values drawn from Poisson distribution.
 *
 * @param fraction the sampling fraction (with replacement)
 * @tparam T item type
 */
private[scio] class PoissonSampler[T](fraction: Double)
  extends RandomSampler[T, IntegerDistribution] {

  /** Epsilon slop to avoid failure from floating point jitter. */
  require(
    fraction >= (0.0 - RandomSampler.roundingEpsilon),
    s"Sampling fraction ($fraction) must be >= 0")

  // PoissonDistribution throws an exception when fraction <= 0
  // If fraction is <= 0, 0 is used below, so we can use any placeholder value.
  override def init: IntegerDistribution = {
    val r = new PoissonDistribution(if (fraction > 0.0) fraction else 1.0)
    if (seed > 0) {
      r.reseedRandomGenerator(seed)
    }
    r
  }

  override def samples: Int = if (fraction <= 0.0) 0 else rng.sample()
}

private[scio] abstract class RandomValueSampler[K, V, R](fractions: Map[K, Double])
  extends DoFn[(K, V), (K, V)] {

  protected var rngs: Map[K, R] = null.asInstanceOf[Map[K, R]]
  protected var seed: Long = -1

  // TODO: is it necessary to setSeed for each instance like Spark does?
  override def startBundle(c: DoFn[(K, V), (K, V)]#Context): Unit = {
    rngs = fractions.mapValues(init).map(identity)  // workaround for serialization issue
  }

  override def processElement(c: DoFn[(K, V), (K, V)]#ProcessContext): Unit = {
    val (key, value) = c.element()
    val count = samples(fractions(key), rngs(key))
    var i = 0
    while (i < count) {
      c.output((key, value))
      i += 1
    }
  }

  def init(fraction: Double): R
  def samples(fraction: Double, rng: R): Int
  def setSeed(seed: Long): Unit = this.seed = seed

}

private[scio] class BernoulliValueSampler[K, V](fractions: Map[K, Double])
  extends RandomValueSampler[K, V, JRandom](fractions) {

  /** Epsilon slop to avoid failure from floating point jitter */
  require(
    fractions.values.forall { f =>
      f >= (0.0 - RandomSampler.roundingEpsilon) && f <= (1.0 + RandomSampler.roundingEpsilon)
    },
    s"Sampling fractions must be on interval [0, 1]")

  // TODO: is it necessary to setSeed for each instance like Spark does?
  override def init(fraction: Double): JRandom = {
    val r = RandomSampler.newDefaultRNG
    if (seed > 0) {
      r.setSeed(seed)
    }
    r
  }

  override def samples(fraction: Double, rng: JRandom): Int = {
    if (fraction <= 0.0) {
      0
    } else if (fraction >= 1.0) {
      1
    } else {
      if (rng.nextDouble() <= fraction) 1 else 0
    }
  }

}

private[scio] class PoissonValueSampler[K, V](fractions: Map[K, Double])
  extends RandomValueSampler[K, V, IntegerDistribution](fractions) {

  /** Epsilon slop to avoid failure from floating point jitter. */
  require(
    fractions.values.forall(f => f >= (0.0 - RandomSampler.roundingEpsilon)),
    s"Sampling fractions must be >= 0")

  // TODO: is it necessary to setSeed for each instance like Spark does?
  override def init(fraction: Double): IntegerDistribution = {
    val r = new PoissonDistribution(if (fraction > 0.0) fraction else 1.0)
    if (seed > 0) {
      r.reseedRandomGenerator(seed)
    }
    r
  }

  override def samples(fraction: Double, rng: IntegerDistribution): Int =
    if (fraction <= 0.0) 0 else rng.sample()

}
