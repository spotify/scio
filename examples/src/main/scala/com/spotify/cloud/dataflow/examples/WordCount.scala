package com.spotify.cloud.dataflow.examples

import com.spotify.cloud.dataflow._

/*
SBT
runMain
  com.spotify.cloud.dataflow.examples.WordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/dataflow/wordcount
*/

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    val max = context.maxAccumulator[Int]("maxLineLength")
    val min = context.minAccumulator[Int]("minLineLength")
    val sum = context.sumAccumulator[Long]("emptyLines")
    context.textFile(args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt"))
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
      .saveAsTextFile(args("output"))

    context.close()
  }
}
