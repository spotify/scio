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

package com.spotify.scio.tensorflow

import com.google.protobuf.ByteString
import com.spotify.scio.testing.ScioIOSpec
import org.tensorflow.proto.example._

import scala.jdk.CollectionConverters._

object TFSequenceExampleIOTest {
  case class Record(i: Int, ss: Seq[String])

  def toSequenceExample(r: Record): SequenceExample = {
    val context = Features
      .newBuilder()
      .putFeature(
        "i",
        Feature
          .newBuilder()
          .setInt64List(Int64List.newBuilder().addValue(r.i.toLong).build())
          .build()
      )
      .build()
    val fs = r.ss.map { s =>
      Feature
        .newBuilder()
        .setBytesList(
          BytesList
            .newBuilder()
            .addValue(ByteString.copyFromUtf8(s))
            .build()
        )
        .build()
    }
    val featureLists = FeatureLists
      .newBuilder()
      .putFeatureList("ss", FeatureList.newBuilder().addAllFeature(fs.asJava).build())
      .build()
    SequenceExample
      .newBuilder()
      .setContext(context)
      .setFeatureLists(featureLists)
      .build()
  }
}

class TFSequenceExampleIOTest extends ScioIOSpec {
  import TFSequenceExampleIOTest._

  "TFSequenceExampleIO" should "work" in {
    val xs = (1 to 100).map(x => toSequenceExample(Record(x, Seq(x.toString, x.toString))))
    testTap(xs)(_.saveAsTfRecordFile(_))(".tfrecords")
    testJobTest(xs)(TFSequenceExampleIO(_))(_.tfRecordSequenceExampleFile(_))(
      _.saveAsTfRecordFile(_)
    )
  }
}
