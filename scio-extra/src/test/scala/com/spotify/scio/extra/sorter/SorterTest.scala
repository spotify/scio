/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.extra.sorter

import com.spotify.scio.testing._

final class SorterTest extends PipelineSpec {
  "NativeFileSorter" should "sort string keys lexicographically" in {
    val data = Seq(
      ("a", ("a", 1)),
      ("a", ("b", 2)),
      ("b", ("c", 3)),
      ("c", ("d", 4))
    )
    val expected = List(
      ("a", Iterable(("a", 1), ("b", 2))),
      ("b", Iterable(("c", 3))),
      ("c", Iterable(("d", 4)))
    )
    runWithContext { sc =>
      val p = sc
        .parallelize(data)
        .groupByKey
        .sortValues(100)

      p should containInAnyOrder(expected)
    }
  }

  it should "sort byte array keys lexicographically" in {
    val data = Seq(
      ("a", ("a".getBytes, 1)),
      ("a", ("b".getBytes, 2)),
      ("b", ("c".getBytes, 3)),
      ("c", ("d".getBytes, 4))
    )

    val expected = List(
      ("a", Iterable(("a", 1), ("b", 2))),
      ("b", Iterable(("c", 3))),
      ("c", Iterable(("d", 4)))
    )
    runWithContext { sc =>
      val p = sc
        .parallelize(data)
        .groupByKey
        .sortValues(100)
        .mapValues(iter => iter.map(kv => (new String(kv._1), kv._2)))

      p should containInAnyOrder(expected)
    }
  }
}
