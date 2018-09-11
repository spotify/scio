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
import com.spotify.scio.io._
import com.spotify.scio.testing._

class JoinExamplesTest extends PipelineSpec {

  private def eventRow(countryCode: String, sqlDate: String, actor1Name: String,
                       sourceUrl: String) =
    TableRow(
      "ActionGeo_CountryCode" -> countryCode,
      "SQLDATE" -> sqlDate,
      "Actor1Name" -> actor1Name,
      "SOURCEURL" -> sourceUrl)

  private def countryRow(fipscc: String, humanName: String) =
    TableRow("FIPSCC" -> fipscc, "HumanName" -> humanName)

  private def result(countryCode: String, countryName: String,
                     date: String, actor1: String, url: String) =
    s"Country code: $countryCode, Country name: $countryName, " +
      s"Event info: Date: $date, Actor1: $actor1, url: $url"

  val eventData = Seq(
    ("US", "2015-01-01", "Alice", "URL1"),
    ("US", "2015-01-02", "Bob", "URL2"),
    ("UK", "2015-01-03", "Carol", "URL3"),
    ("SE", "2015-01-04", "Dan", "URL4")
  ).map((eventRow _).tupled)

  val countryData = Seq(("US", "United States"), ("UK", "United Kingdom"))
    .map((countryRow _).tupled)

  val expected = Seq(
    ("US", "United States", "2015-01-01", "Alice", "URL1"),
    ("US", "United States", "2015-01-02", "Bob", "URL2"),
    ("UK", "United Kingdom", "2015-01-03", "Carol", "URL3"),
    ("SE", "none", "2015-01-04", "Dan", "URL4")
  ).map((result _).tupled)

  "JoinExamples" should "work" in {
    JobTest[com.spotify.scio.examples.cookbook.JoinExamples.type]
      .args("--output=out.txt")
      .input(BigQueryIO(ExampleData.EVENT_TABLE), eventData)
      .input(BigQueryIO(ExampleData.COUNTRY_TABLE), countryData)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

  "SideInputJoinExamples" should "work" in {
    JobTest[com.spotify.scio.examples.cookbook.SideInputJoinExamples.type]
      .args("--output=out.txt")
      .input(BigQueryIO(ExampleData.EVENT_TABLE), eventData)
      .input(BigQueryIO(ExampleData.COUNTRY_TABLE), countryData)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

  "HashJoinExamples" should "work" in {
    JobTest[com.spotify.scio.examples.cookbook.HashJoinExamples.type]
      .args("--output=out.txt")
      .input(BigQueryIO(ExampleData.EVENT_TABLE), eventData)
      .input(BigQueryIO(ExampleData.COUNTRY_TABLE), countryData)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

}
