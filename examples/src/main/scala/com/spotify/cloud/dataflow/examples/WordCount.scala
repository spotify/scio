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

    context.textFile(args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt"))
      .withAccumulator
      .filter { (l, acc) =>
        val t = l.trim
        acc.max("maxLineLength", t.length).min("minLineLength", t.length)
        val b = t.isEmpty
        if (b) acc.add("emptyLines", 1L)
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
