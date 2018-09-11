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

package com.spotify.scio.examples.extra

import com.spotify.scio.extra.checkpoint._
import com.spotify.scio.io._
import com.spotify.scio.testing._

class CheckpointExampleTest extends PipelineSpec {

  private val input = Seq("foo bar foo", "var")
  private val words = Seq("foo", "foo", "bar", "var")
  private val max = "(foo,2)"
  private val count = Seq("foo" -> 2L, "bar" -> 1L, "var" -> 1L)
  private val prettyCount = count.map{case (k, v) => s"$k: $v"}

  "CheckpointJobTest" should "work" in {
    JobTest[CheckpointExample.type]
      .args("--output=output", "--input=input", "--checkpoint=checkpoint")
      .input(TextIO("input"), input)
      .output(TextIO("output-max"))(_ should containSingleValue (max))
      .output(TextIO("output-words"))(_ should containInAnyOrder (words))
      .output(TextIO("output"))(_ should containInAnyOrder (prettyCount))
      .run()
  }

  it should "work with src checkpoint provided" in {
    JobTest[CheckpointExample.type]
      .args("--output=output", "--input=input", "--checkpoint=checkpoint")
      .input(CheckpointIO("checkpoint-src"), words)
      .output(TextIO("output-max"))(_ should containSingleValue (max))
      .output(TextIO("output-words"))(_ should containInAnyOrder (words))
      .output(TextIO("output"))(_ should containInAnyOrder (prettyCount))
      .run()
  }

  it should "work with src and count checkpoints provided" in {
    JobTest[CheckpointExample.type]
      .args("--output=output", "--input=input", "--checkpoint=checkpoint")
      .input(CheckpointIO("checkpoint-src"), words)
      .input(CheckpointIO("checkpoint-count"), count)
      .output(TextIO("output-max"))(_ should containSingleValue (max))
      .output(TextIO("output-words"))(_ should containInAnyOrder (words))
      .output(TextIO("output"))(_ should containInAnyOrder (prettyCount))
      .run()
  }

}
