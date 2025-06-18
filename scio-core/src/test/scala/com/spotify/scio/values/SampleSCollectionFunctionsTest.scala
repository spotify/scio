/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.values

import com.spotify.scio.BuildInfo
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.random.RandomSamplerUtils

class SampleSCollectionFunctionsTest extends PipelineSpec {

  "SampleSCollection" should "support sample()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 1, 1, 1, 1))
      p.sample(1) should containSingleValue(Iterable(1))
      p.sample(6) should containSingleValue(Iterable(1, 1, 1, 1, 1))
    }
  }

  it should "support sampleBySize()" in {
    if (BuildInfo.scalaVersion.startsWith("2.12")) {
      // Mutation detection fails on PriorityQueue for Scala 2.12
      // hashCode() method always yields an error, since it is not safe to use mutable queues as keys in hash tables.
      succeed
    } else {
      runWithContext { sc =>
        val p = sc.parallelize(Seq[Byte](1, 1, 1, 1, 1))
        p.sampleByteSized(1) should containSingleValue(Iterable[Byte](1))
        p.sampleByteSized(6) should containSingleValue(Iterable[Byte](1, 1, 1, 1, 1))
      }
    }
  }

  it should "support sample() with replacement" in {
    import RandomSamplerUtils._
    for (fraction <- List(0.05, 0.2, 1.0)) {
      val sample = runWithData(population)(_.sample(true, fraction))
      (sample.size.toDouble / populationSize) shouldBe fraction +- 0.05
      sample.toSet.size should be < sample.size
    }
  }

  it should "support sample() without replacement" in {
    import RandomSamplerUtils._
    for (fraction <- List(0.05, 0.2, 1.0)) {
      val sample = runWithData(population)(_.sample(false, fraction))
      (sample.size.toDouble / populationSize) shouldBe fraction +- 0.05
      sample.toSet.size shouldBe sample.size
    }
  }

  it should "support sample() with different seeds without replacement" in {
    import RandomSamplerUtils._

    val sample1 = runWithData(population)(_.sample(false, 0.2, Some(fixedSeed)))
    val sample2 = runWithData(population)(_.sample(false, 0.2, Some(otherSeed)))

    sample1 should not contain theSameElementsAs(sample2)
  }

  it should "support sample() with different seeds with replacement" in {
    import RandomSamplerUtils._

    val sample1 = runWithData(population)(_.sample(true, 0.2, Some(fixedSeed)))
    val sample2 = runWithData(population)(_.sample(true, 0.2, Some(otherSeed)))

    sample1 should not contain theSameElementsAs(sample2)
  }

}
