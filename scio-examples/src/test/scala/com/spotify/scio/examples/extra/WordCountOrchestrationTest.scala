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

package com.spotify.scio.examples.extra

import com.spotify.scio.testing.PipelineSpec

class WordCountOrchestrationTest extends PipelineSpec {

  "countWords" should "count words" in {
    runWithContext { sc =>
      val in = Seq(
        "a b",
        "a b c d",
        "",
        "d e")
      val expected = Seq(
        "a" -> 2L,
        "b" -> 2L,
        "c" -> 1L,
        "d" -> 2L,
        "e" -> 1L)

      val out = WordCountOrchestration.countWords(sc.parallelize(in))
      out should containInAnyOrder (expected)
    }
  }

  "mergeCounts" should "merge counts" in {
    runWithContext { sc =>
      val ins = Seq(
        Seq("a" -> 1L, "b" -> 2L, "c" -> 3L),
        Seq("b" -> 10L, "c" -> 20L, "d" -> 5L))
      val expected = Seq("a" -> 1L, "b" -> 12L, "c" -> 23L, "d" -> 5L)

      val out = WordCountOrchestration.mergeCounts(ins.map(sc.parallelize(_)))
      out should containInAnyOrder (expected)
    }
  }

}
