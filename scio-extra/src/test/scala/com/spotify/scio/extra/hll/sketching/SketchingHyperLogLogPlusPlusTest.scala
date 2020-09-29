package com.spotify.scio.extra.hll.sketching

import com.spotify.scio.extra.hll.HLLSpec

class SketchingHyperLogLogPlusPlusTest extends HLLSpec {

  "SketchHLL++" should "estimate distinct count" in {
    val input = for (i <- 0 to 1000000) yield (i % 20)
    val out = runWithData(input) { scl =>
      scl
        .approximateDistinctCount(new SketchingHyperLogLogPlusPlus(15, 20))
    }

    checkWithErrorRate(out, Seq(20L), 0.5d)
  }

  it should "estimate distinct count per key" in {

    val upperLimit = 1000000
    val in = 0 to upperLimit
    val expt: Seq[(Int, Long)] = for (i <- 0 to 20) yield (i, upperLimit / 20)
    val output: Seq[(Int, Long)] = runWithData(in) { scl =>
      scl
        .keyBy(_ % 20)
        .approximateDistinctCountPerKey(new SketchingHyperLogLogPlusPlus(15, 20))
    }
    checkWithErrorRatePerKey(output, expt, 1.0d)
  }
}
