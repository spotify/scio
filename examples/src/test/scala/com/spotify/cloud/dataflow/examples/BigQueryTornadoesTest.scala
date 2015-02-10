package com.spotify.cloud.dataflow.examples

import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow.testing._

class BigQueryTornadoesTest extends JobSpec {

  val input = Seq(
    (1, true),
    (1, false),
    (2, false),
    (3, true),
    (4, true),
    (4, true)
  ).map(t => TableRow("month" -> t._1, "tornado"-> t._2))

  val expected = Seq((1, 1), (3, 1), (4, 2)).map(t => TableRow("month" -> t._1, "tornado_count" -> t._2))

  "BigQueryTornadoes" should "work" in {
    JobTest("com.spotify.cloud.dataflow.examples.BigQueryTornadoes")
      .args("--output=dataset.table")
      .input(BigQueryIO(BigQueryTornadoes.WEATHER_SAMPLES_TABLE), input)
      .output(BigQueryIO("dataset.table"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
