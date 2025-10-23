/*
 * Copyright 2024 Spotify AB.
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

import com.spotify.scio.bigtable.BigtableIO
import com.spotify.scio.io._
import com.spotify.scio.testing._

class MagnolifyBigtableExampleTest extends PipelineSpec {
  import MagnolifyBigtableExample._

  val project = "my-project"
  val instance = "my-instance"
  val table = "my-table"
  val bigtableOptions: Seq[String] = Seq(
    s"--bigtableProjectId=$project",
    s"--bigtableInstanceId=$instance",
    s"--bigtableTableId=$table"
  )

  val textIn: Seq[String] = Seq("a b c d e", "a b a b")
  val wordCount: Seq[(String, Long)] = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val expected: Seq[(String, WordCount)] = wordCount.map { case (k, v) => (k, WordCount(v)) }
  val expectedText: Seq[String] = expected.map(_.toString)

  "MagnolifyBigtableWriteExample" should "work" in {
    JobTest[MagnolifyBigtableWriteExample.type]
      .args(bigtableOptions :+ "--input=in.txt": _*)
      .input(TextIO("in.txt"), textIn)
      .output(BigtableIO[(String, WordCount)](project, instance, table))(coll =>
        coll should containInAnyOrder(expected)
      )
      .run()
  }

  "MagnolifyBigtableReadExample" should "work" in {
    JobTest[MagnolifyBigtableReadExample.type]
      .args(bigtableOptions :+ "--output=out.txt": _*)
      .input(BigtableIO[(String, WordCount)](project, instance, table), expected)
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(expectedText))
      .run()
  }
}
