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

package com.spotify.scio.transforms

import com.spotify.scio.testing.PipelineSpec

class CustomParallelismSCollectionTest extends PipelineSpec {
  "CustomParallelismSCollection" should "support filterWithParallelism()" in {
    runWithContext { sc =>
      val p =
        sc.parallelize(Seq(1, 2, 3, 4, 5)).filterWithParallelism(1)(_ % 2 == 0)
      p should containInAnyOrder(Seq(2, 4))
    }
  }

  it should "support flatMapWithParallelism()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq("a b c", "d e", "f"))
        .flatMapWithParallelism(1)(_.split(" "))
      p should containInAnyOrder(Seq("a", "b", "c", "d", "e", "f"))
    }
  }

  it should "support mapWithParallelism()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(Seq("1", "2", "3"))
        .mapWithParallelism(2)(_.toInt)
      p should containInAnyOrder(Seq(1, 2, 3))
    }
  }

  it should "support collectWithParallelism" in {
    runWithContext { sc =>
      val records = Seq(
        ("test1", 1),
        ("test2", 2),
        ("test3", 3)
      )
      val p = sc.parallelize(records).collectWithParallelism(1) {
        case ("test2", x) => 2 * x
      }
      p should containSingleValue(4)
    }
  }
}
