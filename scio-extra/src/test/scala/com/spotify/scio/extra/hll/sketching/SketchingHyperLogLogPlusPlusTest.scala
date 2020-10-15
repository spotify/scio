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

package com.spotify.scio.extra.hll.sketching

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.ApproximationAssertions._

class SketchingHyperLogLogPlusPlusTest extends PipelineSpec {

  "SketchHLL++" should "estimate distinct count" in {
    val input = for (i <- 1 to 1000000) yield (i % 20)
    val result = runWithData(input) { scl =>
      scl
        .countApproxDistinct(new SketchingHyperLogLogPlusPlus[Int](15, 20))
    }

    result shouldApproximate withErrorRate(Seq(20L), 0.5d)
  }

  it should "estimate distinct count per key" in {

    val upperLimit = 1000000
    val in = 0 to upperLimit
    val expt = for (i <- 0 until 20) yield (i, (upperLimit / 20).toLong)
    val result = runWithData(in) { scl =>
      scl
        .keyBy(_ % 20)
        .countApproxDistinctByKey(new SketchingHyperLogLogPlusPlus[Int](15, 20))
    }

    result shouldApproximate withErrorRatePerKey(expt, 1.0d)
  }
}
