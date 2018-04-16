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

class MaxPerKeyExamplesTest extends PipelineSpec {

  val input = Seq((1, 10.0), (1, 20.0), (2, 18.0), (3, 19.0), (3, 21.0), (3, 23.0))
    .map(kv => TableRow("month" -> kv._1, "mean_temp" -> kv._2))

  val expected = Seq( (1, 20.0), (2, 18.0), (3, 23.0))
    .map(kv => TableRow("month" -> kv._1, "max_mean_temp" -> kv._2))

  "MaxPerKeyExamples" should "work" in {
    JobTest[com.spotify.scio.examples.cookbook.MaxPerKeyExamples.type]
      .args("--output=dataset.table")
      .input(BigQueryIO(ExampleData.WEATHER_SAMPLES_TABLE), input)
      .output(BigQueryIO[TableRow]("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

}
