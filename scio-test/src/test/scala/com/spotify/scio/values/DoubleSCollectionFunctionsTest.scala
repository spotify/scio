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

package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.StatCounter

class DoubleSCollectionFunctionsTest extends PipelineSpec {

  val ints = 1 to 100
  val longs = ints.map(_.toLong)
  val floats = ints.map(_.toFloat)
  val doubles = ints.map(_.toDouble)
  val expected = StatCounter(ints.map(_.toDouble): _*)

  def test(s: Seq[Double], e: Double): Unit = {
    s.size shouldBe 1L
    s.head shouldBe e +- 1e-10
  }


  "DoubleSCollection" should "support sampleStdev()" in {
    val e = expected.sampleStdev
    test(runWithData(ints)(_.sampleStdev), e)
    test(runWithData(longs)(_.sampleStdev), e)
    test(runWithData(floats)(_.sampleStdev), e)
    test(runWithData(doubles)(_.sampleStdev), e)
  }

  it should "support sampleVariance()" in {
    val e = expected.sampleVariance
    test(runWithData(ints)(_.sampleVariance), e)
    test(runWithData(longs)(_.sampleVariance), e)
    test(runWithData(floats)(_.sampleVariance), e)
    test(runWithData(doubles)(_.sampleVariance), e)
  }

  it should "support stdev()" in {
    val e = expected.stdev
    test(runWithData(ints)(_.stdev), e)
    test(runWithData(longs)(_.stdev), e)
    test(runWithData(floats)(_.stdev), e)
    test(runWithData(doubles)(_.stdev), e)
  }

  it should "support variance()" in {
    val e = expected.variance
    test(runWithData(ints)(_.variance), e)
    test(runWithData(longs)(_.variance), e)
    test(runWithData(floats)(_.variance), e)
    test(runWithData(doubles)(_.variance), e)
  }

}
