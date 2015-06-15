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
      .filter { l =>
        val t = l.trim
        val b = t.isEmpty
        !b
      }
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(args("output"))

    context.close()
  }
}
