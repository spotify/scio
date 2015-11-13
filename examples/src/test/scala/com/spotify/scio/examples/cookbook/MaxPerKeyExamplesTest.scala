package com.spotify.scio.examples.cookbook

import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.testing._

class MaxPerKeyExamplesTest extends PipelineSpec {

  val input = Seq((1, 10.0), (1, 20.0), (2, 18.0), (3, 19.0), (3, 21.0), (3, 23.0))
    .map(kv => TableRow("month" -> kv._1, "mean_temp" -> kv._2))

  val expected = Seq( (1, 20.0), (2, 18.0), (3, 23.0)).map(kv => TableRow("month" -> kv._1, "max_mean_temp" -> kv._2))

  "MaxPerKeyExamples" should "work" in {
    JobTest("com.spotify.scio.examples.cookbook.MaxPerKeyExamples")
      .args("--output=dataset.table")
      .input(BigQueryIO(ExampleData.WEATHER_SAMPLES_TABLE), input)
      .output(BigQueryIO("dataset.table"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
