package com.spotify.scio.extra.hll.zetasketch.distributed

import com.spotify.scio.testing.ApproximationAssertions._
import com.spotify.scio.testing.PipelineSpec

class DistributedZetaSketchHllTest extends PipelineSpec {

  "DistributedZetaSketchHLL" should "approximate distinct count" in {
    val input = for (i <- 0 to 10000) yield (i % 20)
    val output = runWithData(input) { scl =>
      scl.asZetaSketchHLL.sumHll.approxDistinctCount
    }

    output shouldApproximate withErrorRate(Seq(20), 0.5)
  }

  it should "approximate distinct count using aggregator" in {

    val input = for (i <- 0 to 10000) yield (i % 20)
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
        .asZetaSketchHLLByKey
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
