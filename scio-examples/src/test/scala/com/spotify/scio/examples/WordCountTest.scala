/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.examples

import com.spotify.scio.io._
import com.spotify.scio.testing._

class WordCountTest extends PipelineSpec {

  val inData = Seq("a b c d e", "a b a b", "")
  val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")

  "WordCount" should "work" in {
    JobTest[com.spotify.scio.examples.WordCount.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), inData)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

  "MinimalWordCount" should "work" in {
    JobTest[com.spotify.scio.examples.MinimalWordCount.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), inData)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

  "DebuggingWordCount" should "work" in {
    val in = Seq(
      "Flourish a b",
      "Flourish c d",
      "Flourish e",
      "stomach a") ++ (1 to 100).map("x" * _)
    JobTest[com.spotify.scio.examples.DebuggingWordCount.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), in)
      .run()
  }

}
