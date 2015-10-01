package com.spotify.scio.examples

import com.spotify.scio.testing._

class WordCountTest extends PipelineSpec {

  val inData = Seq("a b c d e", "a b a b")
  val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")

  "WordCount" should "work" in {
    JobTest("com.spotify.scio.examples.WordCount")
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), inData)
      .output(TextIO("out.txt"))(_ should equalInAnyOrder (expected))
      .run()
  }

  "MinimalWordCount" should "work" in {
    JobTest("com.spotify.scio.examples.MinimalWordCount")
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), inData)
      .output(TextIO("out.txt"))(_ should equalInAnyOrder (expected))
      .run()
  }

}
