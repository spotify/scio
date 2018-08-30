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

import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.io._
import com.spotify.scio.testing._
import org.joda.time.format.DateTimeFormat

class DistCacheExampleTest extends PipelineSpec {

  val fmt = DateTimeFormat.forPattern("yyyyMMdd")
  def d2t(date: String): Long = fmt.parseDateTime(date).getMillis / 1000

  val in = Seq(
    TableRow("timestamp" -> d2t("20150101")),
    TableRow("timestamp" -> d2t("20150102")),
    TableRow("timestamp" -> d2t("20150103")),
    TableRow("timestamp" -> d2t("20150201")),
    TableRow("timestamp" -> d2t("20150202")),
    TableRow("timestamp" -> d2t("20150301")))

  val distCache = Map(1 -> "Jan", 2 -> "Feb", 3 -> "Mar")

  val expected = Seq("Jan 3", "Feb 2", "Mar 1")

  "DistCacheExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.DistCacheExample.type]
      .args("--output=out.txt")
      .input(TableRowJsonIO(ExampleData.EXPORTED_WIKI_TABLE), in)
      .distCache(DistCacheIO("gs://dataflow-samples/samples/misc/months.txt"), distCache)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

}
