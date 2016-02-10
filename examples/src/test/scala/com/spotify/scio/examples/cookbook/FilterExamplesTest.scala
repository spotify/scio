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
    JobTest("com.spotify.scio.examples.cookbook.FilterExamples")
      .args("--output=dataset.table")
      .input(BigQueryIO(ExampleData.WEATHER_SAMPLES_TABLE), input)
      .output(BigQueryIO("dataset.table"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
