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

package com.spotify.scio.extra

import com.spotify.scio.testing.PipelineSpec

class SparkeyTest extends PipelineSpec {
  "PairSCollectionFunctions" should "support asSparkeySideInput" in {

    import Sparkey._

    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val sideData = Seq(("a", "1"), ("b", "2"), ("c", "3"))
      val p2 = sc.parallelize(sideData).asSparkeySideInput
      val s = p1.withSideInputs(p2).flatMap { (i, si) =>
        val map = si(p2)
        val r = map.toList.map(_._2)
        r
      }.toSCollection
      s should containInAnyOrder (Seq("1", "2", "3"))
    }
  }
}
