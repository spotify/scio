/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.extra.sparkey

import com.spotify.scio.testing.PipelineSpec

class LargeHashSCollectionFunctionsTest extends PipelineSpec {
  it should "support hashFilter() with asLargeSetSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c", "b"))
      val p2 = sc.parallelize(Seq[String]("a", "a", "b", "e")).asLargeSetSideInput
      val p = p1.hashFilter(p2)
      p should containInAnyOrder(Seq("a", "b", "b"))
    }
  }
}
