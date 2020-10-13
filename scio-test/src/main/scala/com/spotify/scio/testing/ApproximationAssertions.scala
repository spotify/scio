/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.testing

import org.scalatest.{Assertion, Succeeded}
import org.scalatest.matchers.should.Matchers._

object ApproximationAssertions {

  implicit class ApproximationAssertionsImplicits[T](private val value: T) extends AnyVal {
    def shouldApproximate(approxAssertion: ApproximationAssertion[T]): Assertion =
      approxAssertion.assert(value)
  }

  /**
   * Trait for unit testing approximation outputs with error rates.
   *
   * A simple test might look like this:
   * {{{
   * import com.spotify.scio.testing.PipelineSpec
   * import com.spotify.scio.testing.ApproximationAssertions._
   *
   * class ApproximatePipelineTest extends PipelineSpec {
   *   "An approximate  pipeline" should "check with error rates" in {
   *     val input: Seq[Int] = ...
   *     val estimator = ZetasketchHllIntCounter()
   *     val output: Seq[Long] = runWithData(input) { sCol =>
   *       sCol.countApproxDistinct(estimator)
   *     }
   *     output shouldApproximate withErrorRate(Seq(3L), 0.5d)
   *   }
   *
   *   it should "works with key-valued" in {
   *     val in: Seq[(String, Int)] = ...
   *     val expected: Seq[(String, Long)] = ...
   *     val estimator = ZetasketchHllIntCounter()
   *     val output = runWithData(in) { scl =>
   *       scl
   *         .countApproxDistinctByKey(estimator)
   *     }
   *     output shouldApproximate withErrorRatePerKey(expected, 0.5d)
   *   }
   * }
   *
   * }}}
   */
  trait ApproximationAssertion[-T] {
    def assert(value: T): Assertion
  }

  /**
   * Check corresponding expected value is off by error rate percentage.
   * i.e.  if actual value is `A`, expected values is `B` with error rate `E`, then assert following.
   *  (B - ((B / 100) * E)) <= A <= (B + ((B / 100) * E)
   *
   *  Assert above for each element pair.
   * @param expected - Expected values, length should be equal to actual.size.
   * @param errorRate - how much percentage off from expected value is acceptable.
   */

  def withErrorRate(
    expected: Iterable[Long],
    errorRate: Double
  ): ApproximationAssertion[Iterable[Long]] = { (actual: Iterable[Long]) =>
    actual.size shouldBe expected.size
    (actual zip expected).foreach { case (act, expt) =>
      val error = ((expt / 100) * errorRate).toLong
      act should be <= (expt + error)
      act should be >= (expt - error)
    }
    Succeeded
  }

  /**
   * Similar to above but works with tuples. Check corresponding expected value is off by error rate percentage.
   * i.e.  if acutal value is `A`, expected values is `B` with error rate `E`, then assert following.
   *  (B - ((B / 100) * E)) <= A <= (B + ((B / 100) * E)
   *
   *  Assert above for each key in the actual.
   * @param expected - Expected (key, values) pairs, length should be equal to actual.size.
   * @param errorRate - how much percentage off from expected value is acceptable.
   */
  def withErrorRatePerKey[K](
    expected: Iterable[(K, Long)],
    errorRate: Double
  ): ApproximationAssertion[Iterable[(K, Long)]] = { (actual: Iterable[(K, Long)]) =>
    actual.size shouldBe expected.size
    (actual zip expected)
    val ex = expected.toMap
    actual.toMap.foreach { case (k, act) =>
      val expt = ex(k)
      val error = ((expt / 100) * errorRate).toLong
      act should be <= (expt + error)
      act should be >= (expt - error)
    }
    Succeeded
  }

}
