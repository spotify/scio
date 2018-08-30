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

import com.google.protobuf.ByteString
import com.spotify.scio.io._
import com.spotify.scio.tensorflow.TFRecordIO
import com.spotify.scio.testing._
import org.tensorflow.example._

class ShapelessTensorFlowExampleTest extends PipelineSpec {

  val textIn = Seq("a b c d e", "a b a b")
  val wordCount = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val examples = wordCount.map { kv =>
    Example.newBuilder().setFeatures(Features.newBuilder()
      .putFeature("word", Feature.newBuilder()
        .setBytesList(BytesList.newBuilder().addValue(ByteString.copyFromUtf8(kv._1))).build())
      .putFeature("count", Feature.newBuilder()
        .setInt64List(Int64List.newBuilder().addValue(kv._2)).build())
    ).build()
  }
  val textOut = wordCount.map(kv => kv._1 + ": " + kv._2)

  "ShapelessTensorFlowWriteExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.ShapelessTensorFlowWriteExample.type]
      .args("--input=in.txt", "--output=wc.tfrecords")
      .input(TextIO("in.txt"), textIn)
      .output(TFRecordIO("wc.tfrecords")) {
        _.map(Example.parseFrom) should containInAnyOrder (examples)
      }
      .run()
  }

  "ShapelessTensorFlowReadExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.ShapelessTensorFlowReadExample.type]
      .args("--input=wc.tfrecords", "--output=out.txt")
      .input(TFRecordIO("wc.tfrecords"), examples.map(_.toByteArray))
      .output(TextIO("out.txt"))(_ should containInAnyOrder (textOut))
      .run()
  }

}
