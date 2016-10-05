/*
 * Copyright 2016 Spotify AB.
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

import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.transforms.DoFnTester

import scala.collection.JavaConverters._

class RandomSamplerTest extends PipelineSpec {

  import RandomSamplerUtils._

  def testSampler(withReplacement: Boolean,
                  expectedFraction: Double, actualFraction: Double): Double = {
    rngSeed.setSeed(fixedSeed)
    val expected = expectedSamples(withReplacement, expectedFraction)

    val sampler = if (withReplacement) {
      new PoissonSampler[Int](actualFraction)
    } else {
      new BernoulliSampler[Int](actualFraction)
    }

    val actual = DoFnTester.of(sampler).processBundle(population.asJava).asScala.toArray
    scala.util.Sorting.quickSort(actual)
    medianKSD(gaps(expected), gaps(actual))
  }

  "PoissonSampler" should "work" in {
    testSampler(true, 0.5, 0.5) should be < D
    testSampler(true, 0.7, 0.7) should be < D
    testSampler(true, 0.9, 0.9) should be < D
    testSampler(true, 0.4, 0.6) should be >= D
  }

  "BernoulliSampler" should "work" in {
    testSampler(false, 0.5, 0.5) should be < D
    testSampler(false, 0.7, 0.7) should be < D
    testSampler(false, 0.9, 0.9) should be < D
    testSampler(false, 0.4, 0.6) should be >= D
  }

  def testValueSampler(withReplacement: Boolean,
                       expectedFraction1: Double, actualFraction1: Double,
                       expectedFraction2: Double, actualFraction2: Double): (Double, Double) = {
    rngSeed.setSeed(fixedSeed)
    val expected = Map(
      "a" -> expectedSamples(withReplacement, expectedFraction1),
      "b" -> expectedSamples(withReplacement, expectedFraction2))

    val fractions = Map("a" -> actualFraction1, "b" -> actualFraction2)
    val sampler = if (withReplacement) {
      new PoissonValueSampler[String, Int](fractions)
    } else {
      new BernoulliValueSampler[String, Int](fractions)
    }

    val actual = DoFnTester.of(sampler).processBundle(keyedPopulation.asJava).asScala
      .groupBy(_._1)
      .mapValues { vs =>
        val a = vs.map(_._2).toArray
        scala.util.Sorting.quickSort(a)
        a
      }
    val k1 = medianKSD(gaps(expected("a")), gaps(actual("a")))
    val k2 = medianKSD(gaps(expected("b")), gaps(actual("b")))
    (k1, k2)
  }

  "PoissonValueSampler" should "work" in {
    val r1 = testValueSampler(true, 0.5, 0.5, 0.9, 0.9)
    r1._1 should be < D
    r1._2 should be < D

    val r2 = testValueSampler(true, 0.9, 0.9, 0.4, 0.6)
    r2._1 should be < D
    r2._2 should be >= D

    val r3 = testValueSampler(true, 0.4, 0.6, 0.9, 0.9)
    r3._1 should be >= D
    r3._2 should be < D

    val r4 = testValueSampler(true, 0.4, 0.6, 0.4, 0.6)
    r4._1 should be >= D
    r4._2 should be >= D
  }

  "BernoulliValueSampler" should "work" in {
    val r1 = testValueSampler(false, 0.5, 0.5, 0.9, 0.9)
    r1._1 should be < D
    r1._2 should be < D

    val r2 = testValueSampler(false, 0.9, 0.9, 0.4, 0.6)
    r2._1 should be < D
    r2._2 should be >= D

    val r3 = testValueSampler(false, 0.4, 0.6, 0.9, 0.9)
    r3._1 should be >= D
    r3._2 should be < D

    val r4 = testValueSampler(false, 0.4, 0.6, 0.4, 0.6)
    r4._1 should be >= D
    r4._2 should be >= D
  }

}
