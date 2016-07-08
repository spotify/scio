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

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.proto.SimpleV2.SimplePB
import com.spotify.scio.proto.Track.TrackPB

// Read protobuf on input, and write another protobuf on output
object ProtobufExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.protobufFile[SimplePB](args("input"))
      .map(p => TrackPB.newBuilder().setTrackId(p.getTrackId).build())
      .saveAsProtobufFile(args("output"))

    sc.close()
  }
}
