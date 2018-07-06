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

package com.spotify.scio.examples.cookbook

import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.testing._

class CombinePerKeyExamplesTest extends PipelineSpec {

  val input = Seq(
    ("c1", "verylongword1"),
    ("c2", "verylongword1"),
    ("c3", "verylongword1"),
    ("c1", "verylongword2"),
    ("c2", "verylongword2"),
    ("c1", "verylongword3"),
    ("c1", "sw1"),
    ("c2", "sw2")
  ).map(kv => TableRow("corpus" -> kv._1, "word" -> kv._2))

  val expected = Seq(
    ("verylongword1", "c1,c2,c3"),
    ("verylongword2", "c1,c2"),
    ("verylongword3", "c1")
  ).map(kv => TableRow("word" -> kv._1, "all_plays" -> kv._2))

  "CombinePerKeyExamples" should "work" in {
    JobTest[com.spotify.scio.examples.cookbook.CombinePerKeyExamples.type]
      .args("--output=dataset.table")
      .input(BigQueryIO[TableRow](ExampleData.SHAKESPEARE_TABLE), input)
      .output(BigQueryIO[TableRow]("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

}
