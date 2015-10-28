package com.spotify.scio.examples

import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio._
/*
SBT
runMain
  com.spotify.scio.examples.WordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/dataflow/wordcount
*/

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt")
    val output = args.optional("output").getOrElse(
      if (sc.options.exists(_.getStagingLocation != null)) {
        GcsPath.fromUri(sc.options.get.getStagingLocation).resolve("counts.txt").toString
      } else {
        throw new IllegalArgumentException("Must specify --output or --stagingLocation")
      })

    val max = sc.maxAccumulator[Int]("maxLineLength")
    val min = sc.minAccumulator[Int]("minLineLength")
    val sum = sc.sumAccumulator[Long]("emptyLines")

    sc.textFile(input)
      .withAccumulator(max, min, sum)
      .filter { (l, c) =>
        val t = l.trim
        c.addValue(max, t.length).addValue(min, t.length)
        val b = t.isEmpty
        if (b) c.addValue(sum, 1L)
        !b
      }
      .toSCollection
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(output)

    sc.close()
  }
}
