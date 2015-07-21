package com.spotify.cloud.dataflow.examples.cookbook

import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
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

    val input = args.getOrElse("input", "gs://dataflow-samples/shakespeare/*")
    val output = args.optional("output").getOrElse(
      if (context.options.getStagingLocation != null) {
        GcsPath.fromUri(context.options.getStagingLocation).resolve("deduped.txt").toString
      } else {
        throw new IllegalArgumentException("Must specify --output or --stagingLocation")
      })

    context.textFile(input)
      .distinct()
      .saveAsTextFile(output)

    context.close()
  }
}
