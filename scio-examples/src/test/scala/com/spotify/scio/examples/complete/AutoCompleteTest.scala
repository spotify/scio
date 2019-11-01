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

package com.spotify.scio.examples.complete

import com.spotify.scio.testing._
import org.joda.time.{Duration, Instant}

class AutoCompleteTest extends PipelineSpec {
  "AutoComplete" should "work" in {
    val data = Seq(
      "apple",
      "apple",
      "apricot",
      "banana",
      "blackberry",
      "blackberry",
      "blackberry",
      "blueberry",
      "blueberry",
      "cherry"
    )
    val expected = Seq(
      ("a", Map("apple" -> 2L, "apricot" -> 1L)),
      ("ap", Map("apple" -> 2L, "apricot" -> 1L)),
      ("b", Map("blackberry" -> 3L, "blueberry" -> 2L)),
      ("ba", Map("banana" -> 1L)),
      ("bl", Map("blackberry" -> 3L, "blueberry" -> 2L)),
      ("c", Map("cherry" -> 1L)),
      ("ch", Map("cherry" -> 1L))
    )
    runWithContext { sc =>
      val in = sc.parallelize(data)
      for (recursive <- Seq(true, false)) {
        val r = AutoComplete
          .computeTopCompletions(in, 2, recursive)
          .filter(_._1.length <= 2)
          .mapValues(_.toMap)
        r should containInAnyOrder(expected)
      }
    }
  }

  it should "work with tiny input" in {
    val data = Seq("x", "x", "x", "xy", "xy", "xyz")
    val expected = Seq(
      ("x", Map("x" -> 3L, "xy" -> 2L)),
      ("xy", Map("xy" -> 2L, "xyz" -> 1L)),
      ("xyz", Map("xyz" -> 1L))
    )
    runWithContext { sc =>
      val in = sc.parallelize(data)
      for (recursive <- Seq(true, false)) {
        val r = AutoComplete
          .computeTopCompletions(in, 2, recursive)
          .mapValues(_.toMap)
        r should containInAnyOrder(expected)
      }
    }
  }

  it should "work with windowed input" in {
    val data = Seq(
      ("xA", new Instant(1)),
      ("xA", new Instant(1)),
      ("xB", new Instant(1)),
      ("xB", new Instant(2)),
      ("xB", new Instant(2))
    )
    val expected = Seq(
      // window [0, 2)
      ("x", Map("xA" -> 2L, "xB" -> 1L)),
      ("xA", Map("xA" -> 2L)),
      ("xB", Map("xB" -> 1L)),
      // window [1, 3)
      ("x", Map("xA" -> 2L, "xB" -> 3L)),
      ("xA", Map("xA" -> 2L)),
      ("xB", Map("xB" -> 3L)),
      // window [2, 3)
      ("x", Map("xB" -> 2L)),
      ("xB", Map("xB" -> 2L))
    )
    runWithContext { sc =>
      val in =
        sc.parallelizeTimestamped(data).withSlidingWindows(new Duration(2))
      for (recursive <- Seq(true, false)) {
        val r = AutoComplete
          .computeTopCompletions(in, 2, recursive)
          .mapValues(_.toMap)
        r should containInAnyOrder(expected)
      }
    }
  }
}
