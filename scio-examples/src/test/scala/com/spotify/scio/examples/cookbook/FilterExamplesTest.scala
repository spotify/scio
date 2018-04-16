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

class FilterExamplesTest extends PipelineSpec {

  private def gsodRow(year: Int, month: Int, day: Int, meanTemp: Double) =
    TableRow("year" -> year, "month" -> month, "day" -> day, "mean_temp" -> meanTemp)

  val input = Seq(
    (2015, 1, 1, 40.0),
    (2015, 2, 1, 50.0),
    (2015, 7, 1, 40.0),
    (2015, 7, 2, 50.0)
  ).map((gsodRow _).tupled)

  val expected = Seq(
    (2015, 7, 1, 40.0)
  ).map((gsodRow _).tupled)

  "FilterExamples" should "work" in {
    JobTest[com.spotify.scio.examples.cookbook.FilterExamples.type]
      .args("--output=dataset.table")
      .input(BigQueryIO(ExampleData.WEATHER_SAMPLES_TABLE), input)
      .output(BigQueryIO[TableRow]("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

}
