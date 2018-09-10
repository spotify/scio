/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.bigtable

import com.google.bigtable.v2.Mutation.SetCell
import com.google.bigtable.v2.{Mutation, Row}
import com.google.protobuf.ByteString
import com.spotify.scio.testing._

class BigtableIOTest extends ScioIOSpec {

  val projectId = "project"
  val instanceId = "instance"

  "BigtableIO" should "work with input" in {
    val xs = (1 to 100).map { x =>
      Row.newBuilder().setKey(ByteString.copyFromUtf8(x.toString)).build()
    }
    testJobTestInput(xs)(
      BigtableIO(projectId, instanceId, _))(_.bigtable(projectId, instanceId, _))
  }

  it should "work with output" in {
    val xs = (1 to 100).map { x =>
      val k = ByteString.copyFromUtf8(x.toString)
      val m = Mutation.newBuilder().setSetCell(
        SetCell.newBuilder().setValue(ByteString.copyFromUtf8(x.toString)))
        .build()
      (k, Iterable(m))
    }
    testJobTestOutput(xs)(
      BigtableIO(projectId, instanceId, _))(_.saveAsBigtable(projectId, instanceId, _))
  }

}
