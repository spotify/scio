package com.spotify.scio.examples.cookbook

import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio._

/*
SBT
runMain
  com.spotify.scio.examples.DeDupExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --output=gs://[BUCKET]/dataflow/de_dup_example
*/

object DeDupExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args.getOrElse("input", "gs://dataflow-samples/shakespeare/*")
    val output = args.optional("output").getOrElse(
      if (sc.options.exists(_.getStagingLocation != null)) {
        GcsPath.fromUri(sc.options.get.getStagingLocation).resolve("deduped.txt").toString
      } else {
        throw new IllegalArgumentException("Must specify --output or --stagingLocation")
      })

    sc.textFile(input)
      .distinct()
      .saveAsTextFile(output)

    sc.close()
  }
}
