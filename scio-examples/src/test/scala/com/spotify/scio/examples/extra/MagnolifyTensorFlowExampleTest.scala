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

package com.spotify.scio.examples.extra

import com.spotify.scio.io._
import com.spotify.scio.tensorflow.TFExampleTypedIO
import com.spotify.scio.testing._

class MagnolifyTensorFlowExampleTest extends PipelineSpec {
  import MagnolifyTensorFlowExample._

  val textIn: Seq[String] = Seq("a b c d e", "a b a b")
  val wordCount: Seq[WordCount] = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
    .map { case (word, count) => WordCount(word, count) }
  val textOut: Seq[String] = wordCount.map(_.toString())

  "MagnolifyTensorFlowWriteExample" should "work" in {
    JobTest[MagnolifyTensorFlowWriteExample.type]
      .args("--input=in.txt", "--output=wc.tfrecords")
      .input(TextIO("in.txt"), textIn)
      .output(TFExampleTypedIO[WordCount]("wc.tfrecords")) {
        _ should containInAnyOrder(wordCount)
      }
      .run()
  }

  "MagnolifyTensorFlowReadExample" should "work" in {
    JobTest[MagnolifyTensorFlowReadExample.type]
      .args("--input=wc.tfrecords", "--output=out.txt")
      .input(TFExampleTypedIO[WordCount]("wc.tfrecords"), wordCount)
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(textOut))
      .run()
  }
}
