package com.spotify.cloud.dataflow.examples.cookbook

import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow.testing._

class FilterExamplesTest extends JobSpec {

  def gsodRow(year: Int, month: Int, day: Int, meanTemp: Double) =
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
    JobTest("com.spotify.cloud.dataflow.examples.cookbook.FilterExamples")
      .args("--output=dataset.table")
      .input(BigQueryIO(FilterExamples.WEATHER_SAMPLES_TABLE), input)
      .output(BigQueryIO("dataset.table"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
