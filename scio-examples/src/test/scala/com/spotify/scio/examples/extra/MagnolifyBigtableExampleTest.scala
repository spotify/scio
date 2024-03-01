package com.spotify.scio.examples.extra

import com.spotify.scio.bigtable.BigtableIO
import com.spotify.scio.io._
import com.spotify.scio.testing._

class MagnolifyBigtableExampleTest extends PipelineSpec {
  import MagnolifyBigtableExample._

  val project = "my-project"
  val instance = "my-instance"
  val table = "my-table"
  val bigtableOptions: Seq[String] = Seq(
    s"--bigtableProjectId=$project",
    s"--bigtableInstanceId=$instance",
    s"--bigtableTableId=$table"
  )

  val textIn: Seq[String] = Seq("a b c d e", "a b a b")
  val wordCount: Seq[(String, Long)] = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val expected: Seq[(String, WordCount)] = wordCount.map { case (k, v) => (k, WordCount(v)) }
  val expectedText: Seq[String] = expected.map(_.toString)

  "MagnolifyBigtableWriteExample" should "work" in {
    JobTest[MagnolifyBigtableWriteExample.type]
      .args(bigtableOptions :+ "--input=in.txt": _*)
      .input(TextIO("in.txt"), textIn)
      .output(BigtableIO[(String, WordCount)](project, instance, table))(coll =>
        coll should containInAnyOrder(expected)
      )
      .run()
  }

  "MagnolifyBigtableReadExample" should "work" in {
    JobTest[MagnolifyBigtableReadExample.type]
      .args(bigtableOptions :+ "--output=out.txt": _*)
      .input(BigtableIO[(String, WordCount)](project, instance, table), expected)
      .output(TextIO("out.txt"))(coll => coll should containInAnyOrder(expectedText))
      .run()
  }
}
