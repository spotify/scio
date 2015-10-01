package com.spotify.scio.examples.cookbook

import com.spotify.scio.testing._

class DeDupExampleTest extends JobSpec {

  val input = Seq("a", "b", "a", "b", "c", "a", "b", "d")

  "DeDupExample" should "work" in {
    JobTest("com.spotify.scio.examples.cookbook.DeDupExample")
      .args("--output=out.txt", "--n=10")
      .input(TextIO("gs://dataflow-samples/shakespeare/*"), input)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (Seq("a", "b", "c", "d")))
      .run()
  }

}
