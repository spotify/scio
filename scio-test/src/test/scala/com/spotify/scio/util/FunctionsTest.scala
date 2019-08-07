/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.util

import com.spotify.scio.ScioContext
import com.twitter.algebird.{Monoid, Semigroup}
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.scalatest._

import scala.collection.JavaConverters._

class FunctionsTest extends FlatSpec with Matchers {

  private def testFn[VA](fn: CombineFn[Int, VA, Int]) = {
    var a1 = fn.createAccumulator()
    var a2 = fn.createAccumulator()
    a1 = fn.addInput(a1, 1)
    a2 = fn.addInput(a2, 10)
    var a3 = fn.mergeAccumulators(Seq(a1, a2).asJava)
    a3 = fn.addInput(a3, 100)
    fn.extractOutput(a3) shouldBe 111
  }

  "Functions" should "work with aggregateFn" in {
    testFn(Functions.aggregateFn[Int, Int](ScioContext(), 0)(_ + _, _ + _))
  }

  it should "work with combineFn" in {
    testFn(Functions.combineFn[Int, Int](ScioContext(), identity, _ + _, _ + _))
  }

  it should "work with reduceFn" in {
    testFn(Functions.reduceFn(ScioContext(), Semigroup.intSemigroup.plus _))
    testFn(Functions.reduceFn(ScioContext(), Semigroup.intSemigroup))
    testFn(Functions.reduceFn(ScioContext(), Monoid.intMonoid))
  }

}
