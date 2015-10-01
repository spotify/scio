package com.spotify.scio.examples

import com.spotify.scio.bigquery._
import com.spotify.scio.testing._

class BigQueryTornadoesTest extends PipelineSpec {

  val input = Seq(
    (1, true),
    (1, false),
    (2, false),
    (3, true),
    (4, true),
    (4, true)
  ).map(t => TableRow("month" -> t._1, "tornado" -> t._2))

  val expected = Seq((1, 1), (3, 1), (4, 2)).map(t => TableRow("month" -> t._1, "tornado_count" -> t._2))

  "BigQueryTornadoes" should "work" in {
    JobTest("com.spotify.scio.examples.BigQueryTornadoes")
      .args("--input=publicdata:samples.gsod", "--output=dataset.table")
      .input(BigQueryIO("publicdata:samples.gsod"), input)
      .output(BigQueryIO("dataset.table"))(_ should equalInAnyOrder (expected))
      .run()
  }

  "TypedBigQueryTornadoes" should "work" in {
    JobTest("com.spotify.scio.examples.TypedBigQueryTornadoes")
      .args("--output=dataset.table")
      .input(BigQueryIO("SELECT tornado, month FROM [publicdata:samples.gsod]"), input)
      .output(BigQueryIO("dataset.table"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
