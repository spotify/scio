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

class SCollectionWithSideOutputTest extends PipelineSpec {

  "SCollectionWithSideOutput" should "support map()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = SideOutput[String]()
      val (main, side) = p1.withSideOutputs(p2).map { (x, s) => s.output(p2, x + "2"); x + "1" }
      main should containInAnyOrder (Seq("a1", "b1", "c1"))
      side(p2) should containInAnyOrder (Seq("a2", "b2", "c2"))
    }
  }

  it should "support flatMap()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = SideOutput[String]()
      val (main, side) = p1.withSideOutputs(p2).flatMap { (x, s) =>
        s.output(p2, x + "2x").output(p2, x + "2y")
        Seq(x + "1x", x + "1y")
      }
      main should containInAnyOrder (Seq("a1x", "a1y", "b1x", "b1y", "c1x", "c1y"))
      side(p2) should containInAnyOrder (Seq("a2x", "a2y", "b2x", "b2y", "c2x", "c2y"))
    }
  }

}
