/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.extra.transforms

import java.util.concurrent.TimeUnit

import com.spotify.scio.testing._

class RateLimiterDoFnTest extends PipelineSpec {

  private val input = 0 until 10

  it should "work" in {
    val timeStart = System.nanoTime()
    runWithContext { sc =>
      val p1 = sc.parallelize(input).withRateLimit(1)
      p1 should containInAnyOrder (input)
    }
    val timeElapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - timeStart)
    timeElapsed should be >= input.size.toLong
  }

  it should "work with larger rate" in {
    val timeStart = System.nanoTime()
    runWithContext { sc =>
      val p1 = sc.parallelize(input).withRateLimit(2)
      p1 should containInAnyOrder (input)
    }
    val timeElapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - timeStart)
    timeElapsed should be >= (input.size / 2).toLong
  }

}
