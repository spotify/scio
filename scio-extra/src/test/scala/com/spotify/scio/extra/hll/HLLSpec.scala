package com.spotify.scio.extra.hll

import com.spotify.scio.testing.PipelineSpec

trait HLLSpec extends PipelineSpec {

  def checkWithErrorRate(actual: Seq[Long], expected: Seq[Long], errorRate: Double): Unit = {
    (actual zip expected)
      .map { case (act, expt) =>
        val error = ((expt / 100) * errorRate).toLong
        act should be <= (expt + error)
        act should be >= (expt - error)
      }
  }

  def checkWithErrorRatePerKey[K](
    actual: Seq[(K, Long)],
    expected: Seq[(K, Long)],
    errorRate: Double
  ): Unit = {
    val ex = expected.toMap
    actual.toMap
      .map { case (k, act) =>
        val expt = ex(k)
        val error = ((expt / 100) * errorRate).toLong
        act should be <= (expt + error)
        act should be >= (expt - error)
      }
  }
}
