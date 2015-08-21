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
    val (context, args) = ContextAndArgs(cmdlineArgs)

    val input = args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt")
    val output = args.optional("output").getOrElse(
      if (context.options.getStagingLocation != null) {
        GcsPath.fromUri(context.options.getStagingLocation).resolve("counts.txt").toString
      } else {
        throw new IllegalArgumentException("Must specify --output or --stagingLocation")
      })

    val max = context.maxAccumulator[Int]("maxLineLength")
    val min = context.minAccumulator[Int]("minLineLength")
    val sum = context.sumAccumulator[Long]("emptyLines")

    context.textFile(input)
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

    context.close()
  }
}
