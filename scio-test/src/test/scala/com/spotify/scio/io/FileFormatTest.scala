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

package com.spotify.scio.io

import java.nio.file.Files

import com.google.protobuf.Timestamp
import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.testing.PipelineSpec

class FileFormatTest extends PipelineSpec {

  // Legacy files generated with 0.3.0-beta1, the last release before the
  // BufferedPrefix{Input,Output}Stream change.

  private val path = "scio-test/src/test/resources"

  private val objects = (1 to 100).map(x => (x, x.toDouble, x % 2 == 0, "s" + x))
  private val protobufs = (1 to 100)
    .map(x => com.google.protobuf.Timestamp.newBuilder.setSeconds(x * 1000).setNanos(x).build)

  // Object file is NOT backwards compatible
  "Object file" should "round trip latest file format" in {
    val temp = Files.createTempDirectory("object-file-")
    Files.delete(temp)

    val sc1 = ScioContext()
    sc1.parallelize(objects).saveAsObjectFile(temp.toString, 1)
    sc1.close()

    val sc2 = ScioContext()
    val p = sc2.objectFile[(Int, Double, Boolean, String)](temp.toString + "/*")
    p should containInAnyOrder (objects)
    sc2.close()
  }

  // Protobuf file IS not backwards compatible
  "Protobuf file" should "work with legacy file format" in {
    val sc = ScioContext()
    val p = sc.protobufFile[Timestamp](s"$path/protobuf-file.avro")
    p should containInAnyOrder (protobufs)
    sc.close()
  }

  it should "round trip latest file format" in {
    val temp = Files.createTempDirectory("object-file-")
    Files.delete(temp)

    val sc1 = ScioContext()
    sc1.parallelize(protobufs).saveAsProtobufFile(temp.toString, 1)
    sc1.close()

    val sc2 = ScioContext()
    val p = sc2.protobufFile[Timestamp](temp.toString + "/*")
    p should containInAnyOrder (protobufs)
    sc2.close()
  }

}
