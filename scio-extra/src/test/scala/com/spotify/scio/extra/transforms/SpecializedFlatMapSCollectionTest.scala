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

import com.spotify.scio.testing.PipelineSpec

class SpecializedFlatMapSCollectionTest extends PipelineSpec {

  "SpecializedFlatMapSCollectionTest" should "support safeFlatMap()" in {
    val errorMsg = "String contains 'a'"
    runWithContext { sc =>
      val (p, errors) = sc.parallelize(Seq("a b c", "d e", "f", "a z y"))
        .safeFlatMap{ e =>
          if (e.contains("a")) {
            throw new Exception(errorMsg)
          } else {
            e.split(" ")
          }
        }
      p should containInAnyOrder (Seq("d", "e", "f"))
      val errorsCleaned = errors.map { case (i, e) => (i, e.getMessage)}
      errorsCleaned should containInAnyOrder (Seq(("a b c", errorMsg), ("a z y", errorMsg)))
    }
  }
}
