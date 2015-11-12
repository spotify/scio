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
  --outputBigqueryTable=[DATASET].streaming_word_extract
*/

object StreamingWordExtract {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.extractOptions[ExampleOptions](cmdlineArgs)
    val sc = ScioContext(opts)

    // set up example wiring
    val dataflowUtils = new DataflowExampleUtils(sc.options)
    dataflowUtils.setup()

    val schema = new TableSchema().setFields(
      List(new TableFieldSchema().setName("string_field").setType("STRING")).asJava)

    sc.pubsubTopic(opts.getPubsubTopic)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .map(_.toUpperCase)
      .map(s => TableRow("string_field" -> s))
      .saveAsBigQuery(args("outputBigqueryTable"), schema)

    val result = sc.close()

    // set up Pubsub topic from input file in an injector pipeline
    args.optional("inputFile").foreach { inputFile =>
      dataflowUtils.runInjectorPipeline(inputFile, opts.getPubsubTopic)
    }

    // CTRL-C to cancel the streaming pipeline
    dataflowUtils.waitToFinish(result.internal)
  }
}
