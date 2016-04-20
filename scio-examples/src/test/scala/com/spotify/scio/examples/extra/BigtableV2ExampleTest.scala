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

import com.spotify.scio.Args
import com.spotify.scio.testing._

class BigtableV2ExampleTest extends PipelineSpec {

  import BigtableV2Example._

  val args = Array(
    "--bigtableProjectId=my-project",
    "--bigtableClusterId=my-cluster",
    "--bigtableZoneId=us-east1-a",
    "--bigtableTableId=my-table")

  val options = bigtableOptions(Args(args))

  val textIn = Seq("a b c d e", "a b a b")
  val wordCount = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val expectedMutations = wordCount.map(kvToSetCell)

  "BigtableV2WriteExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.BigtableV2WriteExample.type]
      .args(args :+ "--input=in.txt": _*)
      .input(TextIO("in.txt"), textIn)
      .output(BigtableOutput("my-table", options)) {
        _ should containInAnyOrder (expectedMutations)
      }
      .run()
  }

  val rowsIn = wordCount.map(kvToRow)
  val expectedText = wordCount.map(kv => kv._1 + ": " + kv._2)

  "BigtableV2ReadExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.BigtableV2ReadExample.type]
      .args(args :+ "--output=out.txt": _*)
      .input(BigtableInput("my-table", options), rowsIn)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expectedText))
      .run()
  }

}
