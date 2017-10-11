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

import com.spotify.scio.testing._

class TypedBigQueryTornadoesTest extends PipelineSpec {

  import TypedBigQueryTornadoes.{Result, Row}

  val inData = Seq(
    Row(Some(true), 1),
    Row(Some(false), 1),
    Row(Some(false), 2),
    Row(Some(true), 3),
    Row(Some(true), 4),
    Row(Some(true), 4)
  ).map(Row.toTableRow)

  val expected = Seq(Result(1, 1), Result(3, 1), Result(4, 2)).map(Result.toTableRow)

  "TypedBigQueryTornadoes" should "work" in {
    JobTest[TypedBigQueryTornadoes.type]
      .args("--output=dataset.table")
      .input(BigQueryIO(TypedBigQueryTornadoes.Row.query), inData)
      .output(BigQueryIO("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

}
