package com.spotify.scio.examples.cookbook

import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.testing._

class CombinePerKeyExamplesTest extends PipelineSpec {

  val input = Seq(
    ("c1", "verylongword1"),
    ("c2", "verylongword1"),
    ("c3", "verylongword1"),
    ("c1", "verylongword2"),
    ("c2", "verylongword2"),
    ("c1", "verylongword3"),
    ("c1", "sw1"),
    ("c2", "sw2")
  ).map(kv => TableRow("corpus" -> kv._1, "word" -> kv._2))

  val expected = Seq(
    ("verylongword1", "c1,c2,c3"),
    ("verylongword2", "c1,c2"),
    ("verylongword3", "c1")
  ).map(kv => TableRow("word" -> kv._1, "all_plays" -> kv._2))

  "CombinePerKeyExamples" should "work" in {
    JobTest("com.spotify.scio.examples.cookbook.CombinePerKeyExamples")
      .args("--output=dataset.table")
      .input(BigQueryIO(ExampleData.SHAKESPEARE_TABLE), input)
      .output(BigQueryIO("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

}
