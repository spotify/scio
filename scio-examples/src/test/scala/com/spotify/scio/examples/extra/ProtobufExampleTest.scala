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

package com.spotify.scio.examples.extra

import com.spotify.scio.proto.SimpleV2.SimplePB
import com.spotify.scio.proto.Track.TrackPB
import com.spotify.scio.avro._
import com.spotify.scio.testing._

class ProtobufExampleTest extends PipelineSpec {

  val input = Seq(
    SimplePB.newBuilder().setPlays(1).setTrackId("track1").build(),
    SimplePB.newBuilder().setPlays(2).setTrackId("track2").build()
  )

  val expected = Seq(
    TrackPB.newBuilder().setTrackId("track1").build(),
    TrackPB.newBuilder().setTrackId("track2").build()
  )

  "ProtobufExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.ProtobufExample.type]
      .args("--input=in.proto", "--output=out.proto")
      .input(ProtobufIO[SimplePB]("in.proto"), input)
      .output(ProtobufIO[TrackPB]("out.proto"))(_ should containInAnyOrder[TrackPB](expected))
      .run()
  }

}
