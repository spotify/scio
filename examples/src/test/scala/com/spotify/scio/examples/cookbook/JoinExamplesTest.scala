package com.spotify.scio.examples.cookbook

import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.testing._

class JoinExamplesTest extends PipelineSpec {

  def eventRow(countryCode: String, sqlDate: String, actor1Name: String, sourceUrl: String) = TableRow(
    "ActionGeo_CountryCode" -> countryCode,
    "SQLDATE" -> sqlDate,
    "Actor1Name" -> actor1Name,
    "SOURCEURL" -> sourceUrl)

  def countryRow(fipscc: String, humanName: String) = TableRow("FIPSCC" -> fipscc, "HumanName" -> humanName)

  def result(countryCode: String, countryName: String, date: String, actor1: String, url: String) =
    s"Country code: $countryCode, Country name: $countryName, Event info: Date: $date, Actor1: $actor1, url: $url"

  val eventData = Seq(
    ("US", "2015-01-01", "Alice", "URL1"),
    ("US", "2015-01-02", "Bob", "URL2"),
    ("UK", "2015-01-03", "Carol", "URL3"),
    ("SE", "2015-01-04", "Dan", "URL4")
  ).map((eventRow _).tupled)

  val countryData = Seq(("US", "United States"), ("UK", "United Kingdom")).map((countryRow _).tupled)

  val expected = Seq(
    ("US", "United States", "2015-01-01", "Alice", "URL1"),
    ("US", "United States", "2015-01-02", "Bob", "URL2"),
    ("UK", "United Kingdom", "2015-01-03", "Carol", "URL3"),
    ("SE", "none", "2015-01-04", "Dan", "URL4")
  ).map((result _).tupled)

  "JoinExamples" should "work" in {
    JobTest("com.spotify.scio.examples.cookbook.JoinExamples")
      .args("--output=out.txt")
      .input(BigQueryIO(ExampleData.EVENT_TABLE), eventData)
      .input(BigQueryIO(ExampleData.COUNTRY_TABLE), countryData)
      .output(TextIO("out.txt"))(_ should equalInAnyOrder (expected))
      .run()
  }

  "SideInputJoinExamples" should "work" in {
    JobTest("com.spotify.scio.examples.cookbook.SideInputJoinExamples")
      .args("--output=out.txt")
      .input(BigQueryIO(ExampleData.EVENT_TABLE), eventData)
      .input(BigQueryIO(ExampleData.COUNTRY_TABLE), countryData)
      .output(TextIO("out.txt"))(_ should equalInAnyOrder (expected))
      .run()
  }

  "HashJoinExamples" should "work" in {
    JobTest("com.spotify.scio.examples.cookbook.HashJoinExamples")
      .args("--output=out.txt")
      .input(BigQueryIO(ExampleData.EVENT_TABLE), eventData)
      .input(BigQueryIO(ExampleData.COUNTRY_TABLE), countryData)
      .output(TextIO("out.txt"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
