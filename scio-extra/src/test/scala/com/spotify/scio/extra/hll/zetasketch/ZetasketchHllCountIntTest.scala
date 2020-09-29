package com.spotify.scio.extra.hll.zetasketch

import com.spotify.scio.extra.hll.HLLSpec

class ZetasketchHllCountIntTest extends HLLSpec {

  "ZeetasketchHLL++" should "estimate distinct count" in {
    val estimator = ZetasketchHllCountInt()
    val input = for (i <- 0 to 1000000) yield (i % 20)
    val output = runWithData(input) { scl =>
      scl
        .approximateDistinctCount(estimator)
    }
    checkWithErrorRate(output, Seq(20L), 0.6d)
  }

  it should "estimate distinct count per key" in {
    val estimator = ZetasketchHllCountInt()
    val upperLimit = 10000
    val in = 0 to upperLimit
    val expt: Seq[(Int, Long)] = for (i <- 0 to 5) yield (i, upperLimit / 5)
    val output = runWithData(in) { scl =>
      scl
        .keyBy(_ % 5)
        .approximateDistinctCountPerKey(estimator)
    }

    checkWithErrorRatePerKey(output, expt, 0.5d)
  }
}
