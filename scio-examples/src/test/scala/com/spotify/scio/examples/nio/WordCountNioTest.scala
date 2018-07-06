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

package com.spotify.scio.examples.nio

import com.spotify.scio.nio
import com.spotify.scio.testing._

class WordCountNioTest extends PipelineSpec {

  val inData = Seq("a b c d e", "a b a b", "")
  val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")

  "WordCount" should "works with nio JobTest inputs" in {
    JobTest[com.spotify.scio.examples.nio.WordCountNio.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(nio.TextIO("in.txt"), inData)
      .output[String](nio.TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }
}
