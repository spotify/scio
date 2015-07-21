package com.spotify.cloud.dataflow.examples.cookbook

import com.spotify.cloud.dataflow.testing._

class DeDupExampleTest extends JobSpec {

  val input = Seq("a", "b", "a", "b", "c", "a", "b", "d")

  "DeDupExample" should "work" in {
    JobTest("com.spotify.cloud.dataflow.examples.cookbook.DeDupExample")
      .args("--output=out.txt", "--n=10")
      .input(TextIO("gs://dataflow-samples/shakespeare/*"), input)
      .output(TextIO("out.txt"))(_ should containInAnyOrder ("a", "b", "c", "d"))
      .run()
  }

}
