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

package com.spotify.scio.accumulators

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.Accumulator
import org.apache.commons.lang.exception.ExceptionUtils

class AccumulatorSCollectionTest extends PipelineSpec {

  "AccumulatorSCollection" should "support accumulate" in {
    val sc = ScioContext.forTest()

    val max = sc.maxAccumulator[Int]("max")
    val min = sc.minAccumulator[Int]("min")
    val sum = sc.sumAccumulator[Int]("sum")
    sc.parallelize(1 to 100).accumulate(max, min, sum)
    val r = sc.close()

    r.accumulatorTotalValue(max) shouldBe 100
    r.accumulatorTotalValue(min) shouldBe 1
    r.accumulatorTotalValue(sum) shouldBe 5050
  }

  it should "support accumulateBy" in {
    val sc = ScioContext.forTest()

    val max = sc.maxAccumulator[Int]("max")
    val min = sc.minAccumulator[Int]("min")
    val sum = sc.sumAccumulator[Int]("sum")
    sc.parallelize(Seq("a", "ab", "abc")).accumulateBy(max, min, sum)(_.length)
    val r = sc.close()

    r.accumulatorTotalValue(max) shouldBe 3
    r.accumulatorTotalValue(min) shouldBe 1
    r.accumulatorTotalValue(sum) shouldBe 6
  }

  it should "support accumulateCount" in {
    val sc = ScioContext.forTest()

    val sum = sc.sumAccumulator[Long]("sum")
    sc.parallelize(1 to 100).accumulateCount(sum)
    val r = sc.close()

    r.accumulatorTotalValue(sum) shouldBe 100L
  }

  it should "support accumulateCount with default accumulator" in {
    val sc = ScioContext.forTest()

    sc.parallelize(1 to 100).accumulateCount
    val r = sc.close()

    val as = r.accumulators
    as.size shouldBe 1
    as.head.name should startWith ("accumulateCount@{AccumulatorSCollectionTest.scala")
    r.accumulatorTotalValue(as.head.asInstanceOf[Accumulator[Long]]) shouldBe 100L
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail accumulateCount with incorrect accumulators" in {
    val sc = ScioContext.forTest()

    val max = sc.maxAccumulator[Long]("max")
    val min = sc.minAccumulator[Long]("min")
    the [IllegalArgumentException] thrownBy {
      sc.parallelize(1 to 100).accumulateCount(max)
    } should have message "requirement failed: acc must be a sum accumulator"
    the [IllegalArgumentException] thrownBy {
      sc.parallelize(1 to 100).accumulateCount(min)
    } should have message "requirement failed: acc must be a sum accumulator"
  }
  // scalastyle:on no.whitespace.before.left.bracket

  it should "support accumulateCountFilter" in {
    val sc = ScioContext.forTest()

    val pos = sc.sumAccumulator[Long]("pos")
    val neg = sc.sumAccumulator[Long]("neg")
    sc.parallelize(1 to 100).accumulateCountFilter(pos, neg)(_ % 3 == 0)
    val r = sc.close()

    r.accumulatorTotalValue(pos) shouldBe 33
    r.accumulatorTotalValue(neg) shouldBe 67
  }

  it should "support accumulateCountFilter with default accumulators" in {
    val sc = ScioContext.forTest()

    sc.parallelize(1 to 100).accumulateCountFilter(_ % 3 == 0)
    val r = sc.close()

    val as = r.accumulators
    as.size shouldBe 2
    val accPos = as
      .find(_.name.startsWith("Positive#accumulateCountFilter@{AccumulatorSCollectionTest.scala"))
      .get.asInstanceOf[Accumulator[Long]]
    val accNeg = as
      .find(_.name.startsWith("Negative#accumulateCountFilter@{AccumulatorSCollectionTest.scala"))
      .get.asInstanceOf[Accumulator[Long]]
    r.accumulatorTotalValue(accPos) shouldBe 33
    r.accumulatorTotalValue(accNeg) shouldBe 67
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail accumulateCountFilter with incorrect accumulators" in {
    val sc = ScioContext.forTest()

    val max = sc.maxAccumulator[Long]("max")
    val min = sc.minAccumulator[Long]("min")
    val sum = sc.sumAccumulator[Long]("sum")
    the [IllegalArgumentException] thrownBy {
      sc.parallelize(1 to 100).accumulateCountFilter(max, sum)(_ % 3 == 0)
    } should have message "requirement failed: accPos must be a sum accumulator"
    the [IllegalArgumentException] thrownBy {
      sc.parallelize(1 to 100).accumulateCountFilter(sum, min)(_ % 3 == 0)
    } should have message "requirement failed: accNeg must be a sum accumulator"
  }
  // scalastyle:on no.whitespace.before.left.bracket

}
