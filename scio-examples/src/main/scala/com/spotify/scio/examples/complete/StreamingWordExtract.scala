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

// Example: Streaming Word Extract
// Usage:

// `sbt runMain "com.spotify.scio.examples.complete.StreamingWordExtract
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=[DATASET].streaming_word_extract"`
package com.spotify.scio.examples.complete

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.options.StreamingOptions

import scala.collection.JavaConverters._

object StreamingWordExtract {
  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    opts.as(classOf[StreamingOptions]).setStreaming(true)
    val exampleUtils = new ExampleUtils(opts)

    val sc = ScioContext(opts)

    val schema = new TableSchema()
      .setFields(List(new TableFieldSchema().setName("string_field").setType("STRING")).asJava)

    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .map(_.toUpperCase)
      .map(s => TableRow("string_field" -> s))
      .saveAsBigQuery(args("output"), schema)

    val result = sc.close()
    exampleUtils.waitToFinish(result.pipelineResult)
  }
}
