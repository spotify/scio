/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.examples

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils
import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.{ExampleData, ExampleOptions}
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

  val schema = new TableSchema().setFields(List(
    new TableFieldSchema().setName("word").setType("STRING"),
    new TableFieldSchema().setName("count").setType("INTEGER"),
    new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP")
  ).asJava)

  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val dataflowUtils = new DataflowExampleUtils(opts)
    dataflowUtils.setup()

    val sc = ScioContext(opts)

    val inputFile = args.optional("inputFile")
    val windowSize = Duration.standardMinutes(
      args.optional("windowSize").map(_.toLong).getOrElse(WINDOW_SIZE))

    // initialize input
    val input = if (opts.isStreaming) {
      sc.pubsubTopic(opts.getPubsubTopic)
    } else {
      sc
      .textFile(inputFile.getOrElse(ExampleData.KING_LEAR))
      .timestampBy {
        _ => new Instant(System.currentTimeMillis() - (scala.math.random * RAND_RANGE).toLong)
      }
    }

    input
      .withFixedWindows(windowSize)  // apply windowing logic
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .toWindowed  // convert to WindowedSCollection
      .map { wv =>
        wv.copy(value = TableRow(
          "word" -> wv.value._1,
          "count" -> wv.value._2,
          "window_timestamp" -> Timestamp(wv.timestamp.getMillis)))
      }
      .toSCollection  // convert back to normal SCollection
      .saveAsBigQuery(ExampleOptions.bigQueryTable(opts), schema)

    val result = sc.close()

    // set up Pubsub topic from input file in an injector pipeline
    inputFile.foreach { f =>
      dataflowUtils.runInjectorPipeline(f, opts.getPubsubTopic)
    }

    // CTRL-C to cancel the streaming pipeline
    dataflowUtils.waitToFinish(result.internal)
  }

}
