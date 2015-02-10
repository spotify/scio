package com.spotify.cloud.dataflow.examples

import com.spotify.cloud.dataflow._

/*
SBT
runMain
  com.spotify.cloud.dataflow.examples.DeDupExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --output=gs://[BUCKET]/dataflow/de_dup_example
*/

object DeDupExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    context.textFile(args.getOrElse("input", "gs://dataflow-samples/shakespeare/*"))
      .distinct()
      .saveAsTextFile(args("output"), numShards = 1)

    context.close()
  }
}
