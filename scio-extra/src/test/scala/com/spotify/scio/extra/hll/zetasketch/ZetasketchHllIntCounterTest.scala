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

package com.spotify.scio.extra.hll.zetasketch

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.ApproximationAssertions._

class ZetasketchHllIntCounterTest extends PipelineSpec {

  "ZetasketchHLL++" should "estimate int distinct count" in {
    val estimator = ZetasketchHllIntCounter()
    val input = for (i <- 0 to 1000000) yield (i % 20)
    val output = runWithData(input) { scl =>
      scl
        .countApproxDistinct(estimator)
    }
    output shouldApproximate withErrorRate(Seq(20L), 0.6d)
  }

  it should "estimate strings distinct count" in {
    val estimator = ZetasketchHllStringCounter()
    val input = for (i <- 0 to 1000000) yield s"${i % 20}_"
    val output = runWithData(input) { scl =>
      scl
        .countApproxDistinct(estimator)
    }

    output shouldApproximate withErrorRate(Seq(20L), 0.6d)
  }

  it should "estimate longs distinct count" in {
    val estimator = ZetasketchHllLongCounter()
    val input = for (i <- 0 to 1000000) yield ((i % 20).toLong)
    val output = runWithData(input) { scl =>
      scl
        .countApproxDistinct(estimator)
    }
    output shouldApproximate withErrorRate(Seq(20L), 0.6d)
  }

  it should "estimate byte array distinct count" in {
    val estimator = ZetasketchHllByteArrayCounter()
    val input = for (i <- 0 to 1000000) yield (s"${i % 20}_".getBytes)
    val output = runWithData(input) { scl =>
      scl
        .countApproxDistinct(estimator)
    }
    output shouldApproximate withErrorRate(Seq(20L), 0.6d)
  }

  it should "estimate distinct count per key" in {
    val estimator = ZetasketchHllIntCounter()
    val upperLimit = 10000
    val in = 0 to upperLimit
    val expt = for (i <- 0 until 5) yield (i, (upperLimit / 5).toLong)
    val output = runWithData(in) { scl =>
      scl
        .keyBy(_ % 5)
        .countApproxDistinctByKey(estimator)
    }
    output shouldApproximate withErrorRatePerKey(expt, 0.5d)
  }
}
