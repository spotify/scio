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

package com.spotify.scio.examples.complete

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.apache.beam.examples.common.DataflowExampleUtils
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
