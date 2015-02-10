package com.spotify.cloud.dataflow.examples

import com.google.api.services.bigquery.model.TableRow
import com.spotify.cloud.dataflow.testing._

class FilterExamplesTest extends JobSpec {

  def gsodRow(year: Int, month: Int, day: Int, meanTemp: Double) =
    new TableRow().set("year", year).set("month", month).set("day", day).set("mean_temp", meanTemp)

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
    JobTest("com.spotify.cloud.dataflow.examples.FilterExamples")
      .args("--output=dataset.table")
      .input(BigQueryIO(FilterExamples.WEATHER_SAMPLES_TABLE), input)
      .output(BigQueryIO("dataset.table"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
