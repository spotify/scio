package com.spotify.cloud.dataflow.examples

import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow.testing._

class MaxPerKeyExamplesTest extends JobSpec {

  val input = Seq((1, 10.0), (1, 20.0), (2, 18.0), (3, 19.0), (3, 21.0), (3, 23.0))
    .map(kv => TableRow("month" -> kv._1, "mean_temp" -> kv._2))

  val expected = Seq( (1, 20.0), (2, 18.0), (3, 23.0)).map(kv => TableRow("month" -> kv._1, "max_mean_temp" -> kv._2))

  "MaxPerKeyExamples" should "work" in {
    JobTest("com.spotify.cloud.dataflow.examples.MaxPerKeyExamples")
      .args("--output=dataset.table")
      .input(BigQueryIO(MaxPerKeyExamples.WEATHER_SAMPLE_TABLE), input)
      .output(BigQueryIO("dataset.table"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
