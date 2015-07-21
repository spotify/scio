package com.spotify.cloud.dataflow.examples.extra

import com.spotify.cloud.dataflow._

/*
SBT
runMain
  com.spotify.cloud.dataflow.examples.TableRowJsonInOut
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/wikipedia_edits/wiki_data-*.json
  --output=gs://[BUCKET]/dataflow/wikipedia
*/

object TableRowJsonInOut {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    context.tableRowJsonFile(args("input"))
      .take(100)
      .saveAsTableRowJsonFile(args("output"))

    context.close()
  }
}
