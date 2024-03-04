/*
 * Copyright 2019 Spotify AB.
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

// Example: Read and write using typed BigQuery API with case classes
// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.TypedBigQueryTornadoes
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --output=[PROJECT]:[DATASET].[TABLE]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.typed._
import com.spotify.scio.{ContextAndArgs, ScioContext}

object MagnolifyTypedBigQueryTornadoes {
  val query: String = "SELECT tornado, month FROM [bigquery-public-data:samples.gsod]"
  case class Row(tornado: Option[Boolean], month: Long)
  case class Result(month: Long, tornado_count: Long)

  def pipeline(cmdlineArgs: Array[String]): ScioContext = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val resultTap = sc
      // Get input from BigQuery and convert elements from `TableRow` to `Row` with the
      // implicitly-available `TableRowType[Row]`
      .typedBigQuerySelect[Row](Query(query))
      .flatMap(r => if (r.tornado.getOrElse(false)) Seq(r.month) else Nil)
      .countByValue
      .map(kv => Result(kv._1, kv._2))
      // Save output to BigQuery, convert elements from `Result` to `TableRow` with the
      // implicitly-available `TableRowType[Result]`
      .saveAsBigQueryTable(
        Table.Spec(args("output")),
        writeDisposition = WRITE_TRUNCATE,
        createDisposition = CREATE_IF_NEEDED
      )

    // Access the loaded tables
    resultTap
      .output(BigQueryIO.SuccessfulTableLoads)
      .map(_.getTableSpec)
      .debug(prefix = "Loaded table: ")

    // Access the failed records
    resultTap
      .output(BigQueryIO.FailedInserts)
      .count
      .debug(prefix = "Failed inserts: ")

    sc
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val sc = pipeline(cmdlineArgs)
    sc.run().waitUntilDone()
    ()
  }
}
