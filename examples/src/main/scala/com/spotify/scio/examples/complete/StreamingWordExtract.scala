package com.spotify.scio.examples.complete

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleOptions

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.complete.StreamingWordExtract
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --streaming=true
  --pubsubTopic=projects/[PROJECT]/topics/streaming_word_extract
  --inputFile=gs://dataflow-samples/shakespeare/kinglear.txt
  --bigQueryDataset=[DATASET]
  --bigQueryTable=[TABLE]
*/

object StreamingWordExtract {
  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val dataflowUtils = new DataflowExampleUtils(opts)
    dataflowUtils.setup()

    val sc = ScioContext(opts)

    val schema = new TableSchema().setFields(
      List(new TableFieldSchema().setName("string_field").setType("STRING")).asJava)

    sc.pubsubTopic(opts.getPubsubTopic)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .map(_.toUpperCase)
      .map(s => TableRow("string_field" -> s))
      .saveAsBigQuery(ExampleOptions.bigQueryTable(opts), schema)

    val result = sc.close()

    // set up Pubsub topic from input file in an injector pipeline
    args.optional("inputFile").foreach { f =>
      dataflowUtils.runInjectorPipeline(f, opts.getPubsubTopic)
    }

    // CTRL-C to cancel the streaming pipeline
    dataflowUtils.waitToFinish(result.internal)
  }
}
