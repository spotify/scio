/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec

class MutationTest extends PipelineSpec {
  "BigDecimal serialization" should "not cause mutation exceptions" in {
    runWithContext { sc =>
      val expected = (1 to 10).map(_ * 2).toList
      val result = sc
        .parallelize(1 to 10)
        .map(BigDecimal(_))
        .map(_ * 2)
        .map(_.toInt)
      result should containInAnyOrder(expected)
    }
  }
}
