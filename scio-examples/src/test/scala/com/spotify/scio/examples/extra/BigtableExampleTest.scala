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

import com.google.bigtable.v2.{Mutation, Row}
import com.google.protobuf.ByteString
import com.spotify.scio.bigtable._
import com.spotify.scio.io._
import com.spotify.scio.testing._

class BigtableExampleTest extends PipelineSpec {

  import BigtableExample._

  val bigtableOptions = Seq(
    "--bigtableProjectId=my-project",
    "--bigtableInstanceId=my-instance",
    "--bigtableTableId=my-table")

  val textIn = Seq("a b c d e", "a b a b")
  val wordCount = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val expectedMutations = wordCount.map(kv => BigtableExample.toMutation(kv._1, kv._2))

  "BigtableV1WriteExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.BigtableWriteExample.type]
      .args(bigtableOptions :+ "--input=in.txt": _*)
      .input(TextIO("in.txt"), textIn)
      .output(BigtableIO[(ByteString, Iterable[Mutation])](
        "my-project", "my-instance", "my-table")) {
        _ should containInAnyOrder (expectedMutations)
      }
      .run()
  }


  def toRow(key: String, value: Long): Row =
    Rows.newRow(
      ByteString.copyFromUtf8(key), FAMILY_NAME, COLUMN_QUALIFIER,
      ByteString.copyFromUtf8(value.toString))

  val rowsIn = wordCount.map(kv => toRow(kv._1, kv._2))
  val expectedText = wordCount.map(kv => kv._1 + ": " + kv._2)

  "BigtableReadExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.BigtableReadExample.type]
      .args(bigtableOptions :+ "--output=out.txt": _*)
      .input(BigtableIO[Row]("my-project", "my-instance", "my-table"), rowsIn)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expectedText))
      .run()
  }

}
