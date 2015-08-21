package com.spotify.scio.examples

import com.spotify.scio._

/*
SBT
runMain
  com.spotify.scio.examples.MinimalWordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/dataflow/minimal_wordcount
*/

object MinimalWordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.textFile(args.getOrElse("input", "gs://dataflow-samples/shakespeare/*"))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
