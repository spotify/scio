// INCOMPLETE
package com.spotify.scio.examples

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils
import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleOptions
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.WindowedWordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --streaming=true
  --pubsubTopic=projects/[PROJECT]/topics/windowed_word_count
  --inputFile=gs://dataflow-samples/shakespeare/kinglear.txt
  --bigQueryDataset=[DATASET]
  --bigQueryTable=[TABLE]
*/

object WindowedWordCount {

  val RAND_RANGE = 7200000
  val WINDOW_SIZE = 1

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val sc = ScioContext(opts)

    // set up example wiring
    val dataflowUtils = new DataflowExampleUtils(sc.options)
    dataflowUtils.setup()

    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING"),
      new TableFieldSchema().setName("count").setType("INTEGER"),
      new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP")
    ).asJava)

    val inputFile = args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt")
    val windowSize = Duration.standardMinutes(args.optional("windowSize").map(_.toLong).getOrElse(WINDOW_SIZE))

    // initialize input
    val input = if (opts.isStreaming) {
      sc.pubsubTopic(opts.getPubsubTopic)
    } else {
      sc
      .textFile(inputFile)
      .toWindowed  // convert to WindowedSCollection
      .map { wv =>  // specialized version of map with WindowedValue as argument
        // update timestamp of elements
        val randomTimestamp = System.currentTimeMillis() - (scala.math.random * RAND_RANGE).toLong
        wv.copy(value = wv.value, timestamp = new Instant(randomTimestamp))
      }
      .toSCollection  // convert back to normal SCollection
    }

    input
      .withFixedWindows(windowSize)  // apply windowing logic
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .toWindowed  // convert to WindowedSCollection
      .map { wv =>
        wv.copy(value = TableRow(
          "word" -> wv.value._1,
          "count" -> wv.value._2,
          "window_timestamp" -> Timestamp(wv.timestamp)))
      }
      .toSCollection  // convert back to normal SCollection
      .saveAsBigQuery(ExampleOptions.bigQueryTable(opts), schema)

    val result = sc.close()

    // set up Pubsub topic from input file in an injector pipeline
    args.optional("inputFile").foreach { inputFile =>
      dataflowUtils.runInjectorPipeline(inputFile, opts.getPubsubTopic)
    }

    // CTRL-C to cancel the streaming pipeline
    dataflowUtils.waitToFinish(result.internal)
  }

}
