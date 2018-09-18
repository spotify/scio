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

package com.spotify.scio.examples.complete

import com.spotify.scio.testing._

class TfIdfTest extends PipelineSpec {

  "TfIdf.computeTfIdf" should "work" in {
    val data = Seq(("x", "a b c d"), ("y", "a b c"), ("z", "a m n"))
    runWithContext { sc =>
      val p = TfIdf.computeTfIdf(sc.parallelize(data))
      p.keys.distinct should containInAnyOrder(Seq("a", "m", "n", "b", "c", "d"))
    }
  }

}
