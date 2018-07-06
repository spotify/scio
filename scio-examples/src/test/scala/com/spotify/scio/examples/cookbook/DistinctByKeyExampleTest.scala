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

package com.spotify.scio.examples.cookbook

import com.spotify.scio.bigquery._
import com.spotify.scio.testing._

class DistinctByKeyExampleTest extends PipelineSpec {

  val input = Seq(
    ("c1", "verylongword1"),
    ("c1", "verylongword1"),
    ("c1", "verylongword1"),
    ("c2", "verylongword2"),
    ("c2", "verylongword2"),
    ("c2", "verylongword3"),
    ("c1", "sw1"),
    ("c2", "sw2")
  ).map(kv => TableRow("corpus" -> kv._1, "word" -> kv._2))

  val expected = Seq(
    ("verylongword1", "c1"),
    ("verylongword2", "c2"),
    ("verylongword3", "c2")
  ).map(kv => TableRow("word" -> kv._1, "reference_play" -> kv._2))

  "DistinctByKeyExample" should "work" in {
    JobTest[com.spotify.scio.examples.cookbook.DistinctByKeyExample.type]
      .args("--input=input.table", "--output=dataset.table")
      .input(BigQueryIO[TableRow]("input.table"), input)
      .output(BigQueryIO[TableRow]("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

}
