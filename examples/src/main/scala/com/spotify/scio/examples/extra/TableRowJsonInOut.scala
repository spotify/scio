package com.spotify.scio.examples.extra

import com.spotify.scio._

/*
SBT
runMain
  com.spotify.scio.examples.TableRowJsonInOut
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/wikipedia_edits/wiki_data-*.json
  --output=gs://[BUCKET]/dataflow/wikipedia
*/

object TableRowJsonInOut {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.tableRowJsonFile(args("input"))
      .take(100)
      .saveAsTableRowJsonFile(args("output"))

    sc.close()
  }
}
