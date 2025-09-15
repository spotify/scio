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

import com.spotify.scio.testing.ApproximationAssertions._
import com.spotify.scio.testing.PipelineSpec

class DistributedZetaSketchHllTest extends PipelineSpec {

  "DistributedZetaSketchHll" should "approximate distinct count" in {
    val input = for (i <- 0 to 10000) yield i % 20
    val output = runWithData(input) { scl =>
      scl.asZetaSketchHll.sumHll.approxDistinctCount
    }

    output shouldApproximate withErrorRate(Seq(20), 0.5)
  }

  it should "approximate distinct count using aggregator" in {

    val input = for (i <- 0 to 10000) yield i % 20
    val output = runWithData(input) { scl =>
      scl.approxDistinctCountWithZetaHll
    }

    output shouldApproximate withErrorRate(Seq(20), 0.5)
  }

  it should "approximate key-value distinct count" in {
    val upperLimit = 10000
    val in = 0 to upperLimit
    val expt = for (i <- 0 until 5) yield (i, (upperLimit / 5).toLong)
    val output = runWithData(in) { scl =>
      scl
        .keyBy(_ % 5)
        .asZetaSketchHllByKey
        .sumHllByKey
        .approxDistinctCountByKey
    }
    output shouldApproximate withErrorRatePerKey(expt, 0.5)
  }

  it should "approximate key-value distinct count with aggregator" in {
    val upperLimit = 10000
    val in = 0 to upperLimit
    val expt = for (i <- 0 until 5) yield (i, (upperLimit / 5).toLong)
    val output = runWithData(in) { scl =>
      scl
        .keyBy(_ % 5)
        .approxDistinctCountWithZetaHllByKey
    }
    output shouldApproximate withErrorRatePerKey(expt, 0.5)
  }
}
