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

package com.spotify.scio.examples.cookbook

import com.spotify.scio.io._
import com.spotify.scio.testing._

class DistinctExampleTest extends PipelineSpec {

  val input = Seq(
    "word1",
    "word1",
    "word2",
    "word3",
    "word3"
  )

  val expected = Seq(
    "word1",
    "word2",
    "word3"
  )

  "DistinctExample" should "work" in {
    JobTest[com.spotify.scio.examples.cookbook.DistinctExample.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), input)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

}
