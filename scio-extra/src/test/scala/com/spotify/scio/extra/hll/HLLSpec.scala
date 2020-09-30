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

package com.spotify.scio.extra.hll

import com.spotify.scio.testing.PipelineSpec

trait HLLSpec extends PipelineSpec {

  def checkWithErrorRate(actual: Seq[Long], expected: Seq[Long], errorRate: Double): Unit = {
    (actual zip expected)
      .foreach { case (act, expt) =>
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
      .foreach { case (k, act) =>
        val expt = ex(k)
        val error = ((expt / 100) * errorRate).toLong
        act should be <= (expt + error)
        act should be >= (expt - error)
      }
  }
}
