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

import java.nio.file.Paths

import com.spotify.scio.ScioContext
import com.spotify.scio.proto.SimpleV2.SimplePB
import com.spotify.scio.testing._
import com.spotify.scio.avro._

class BackcompatibilityTest extends PipelineSpec {
  val input: Seq[SimplePB] = Seq(
    SimplePB.newBuilder().setPlays(1).setTrackId("track1").build(),
    SimplePB.newBuilder().setPlays(2).setTrackId("track2").build()
  )

  val pwd: String = Paths.get(".").toAbsolutePath.toString
  val path_07 = "scio-examples/src/test/resources/scio-0.7-protobuf"
  val path_06 = "scio-examples/src/test/resources/scio-0.6-protobuf"
  // The protobuf files for that tests were generated using
  // the following code running on different scio versions:
  // import com.spotify.scio.proto.SimpleV2.SimplePB
  // import com.spotify.scio.ContextAndArgs
  // val (sc, args) = ContextAndArgs(Array())
  // val input = Seq(
  //   SimplePB.newBuilder().setPlays(1).setTrackId("track1").build(),
  //   SimplePB.newBuilder().setPlays(2).setTrackId("track2").build()
  // )
  // sc.parallelize(input).saveAsProtobufFile(path_07)
  // sc.run()
  "saveAsProtobuf" should "read protobuf files written with Scio 0.7 and above" in {
    val sc = ScioContext()
    val r = sc.protobufFile[SimplePB](s"$pwd/$path_07", ".protobuf.avro")
    r should containInAnyOrder(input)
    sc.run()
  }

  it should "read protobuf files written with Scio 0.6 and below" in {
    val sc = ScioContext()
    val r = sc.protobufFile[SimplePB](s"$pwd/$path_06", ".protobuf.avro")
    r should containInAnyOrder(input)
    sc.run()
  }
}
