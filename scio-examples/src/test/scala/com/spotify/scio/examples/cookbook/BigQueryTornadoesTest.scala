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
import com.spotify.scio.testing._

class BigQueryTornadoesTest extends PipelineSpec {

  val inData = Seq(
    (1, true),
    (1, false),
    (2, false),
    (3, true),
    (4, true),
    (4, true)
  ).map(t => TableRow("month" -> t._1, "tornado" -> t._2))

  val expected = Seq((1, 1), (3, 1), (4, 2))
    .map(t => TableRow("month" -> t._1, "tornado_count" -> t._2))

  "BigQueryTornadoes" should "work" in {
    JobTest[com.spotify.scio.examples.cookbook.BigQueryTornadoes.type]
      .args("--input=publicdata:samples.gsod", "--output=dataset.table")
      .input(BigQueryIO("publicdata:samples.gsod"), inData)
      .output(BigQueryIO[TableRow]("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

}
