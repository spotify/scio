package com.spotify.scio.extra.hll.zetasketch

import com.spotify.scio.extra.hll.HLLSpec

class ZetasketchHllIntCounterTest extends HLLSpec {

  "ZetasketchHLL++" should "estimate int distinct count" in {
    val estimator = ZetasketchHllIntCounter()
    val input = for (i <- 0 to 1000000) yield (i % 20)
    val output = runWithData(input) { scl =>
      scl
        .countApproxDistinct(estimator)
    }
    checkWithErrorRate(output, Seq(20L), 0.6d)
  }

  it should "estimate strings distinct count" in {
    val estimator = ZetasketchHllStringCounter()
    val input = for (i <- 0 to 1000000) yield s"${i % 20}_"
    val output = runWithData(input) { scl =>
      scl
        .countApproxDistinct(estimator)
    }
    checkWithErrorRate(output, Seq(20L), 0.6d)
  }

  it should "estimate longs distinct count" in {
    val estimator = ZetasketchHllLongCounter()
    val input = for (i <- 0 to 1000000) yield ((i % 20).toLong)
    val output = runWithData(input) { scl =>
      scl
        .countApproxDistinct(estimator)
    }
    checkWithErrorRate(output, Seq(20L), 0.6d)
  }

  it should "estimate byte array distinct count" in {
    val estimator = ZetasketchHllByteArrayCounter()
    val input = for (i <- 0 to 1000000) yield (s"${i % 20}_".getBytes)
    val output = runWithData(input) { scl =>
      scl
        .countApproxDistinct(estimator)
    }
    checkWithErrorRate(output, Seq(20L), 0.6d)
  }

  it should "estimate distinct count per key" in {
    val estimator = ZetasketchHllIntCounter()
    val upperLimit = 10000
    val in = 0 to upperLimit
    val expt: Seq[(Int, Long)] = for (i <- 0 to 5) yield (i, upperLimit / 5)
    val output = runWithData(in) { scl =>
      scl
        .keyBy(_ % 5)
        .countApproxDistinctByKey(estimator)
    }
    checkWithErrorRatePerKey(output, expt, 0.5d)
  }
}
