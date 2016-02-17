package com.spotify.scio.examples.extra

import com.spotify.scio.testing.PipelineSpec

class WordCountOrchestrationTest extends PipelineSpec {

  "countWords" should "count words" in {
    runWithContext { sc =>
      val in = Seq(
        "a b",
        "a b c d",
        "",
        "d e")
      val expected = Seq(
        "a" -> 2L,
        "b" -> 2L,
        "c" -> 1L,
        "d" -> 2L,
        "e" -> 1L)

      val out = WordCountOrchestration.countWords(sc.parallelize(in))
      out should containInAnyOrder (expected)
    }
  }

  "mergeCounts" should "merge counts" in {
    runWithContext { sc =>
      val ins = Seq(
        Seq("a" -> 1L, "b" -> 2L, "c" -> 3L),
        Seq("b" -> 10L, "c" -> 20L, "d" -> 5L))
      val expected = Seq("a" -> 1L, "b" -> 12L, "c" -> 23L, "d" -> 5L)

      val out = WordCountOrchestration.mergeCounts(ins.map(sc.parallelize(_)))
      out should containInAnyOrder (expected)
    }
  }

}
