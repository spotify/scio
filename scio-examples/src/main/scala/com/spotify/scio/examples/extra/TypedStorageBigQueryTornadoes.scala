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

// Example: Read using typed BigQuery Storage API with annotated case classes
// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.TypedStorageBigQueryTornadoes
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --output=[PROJECT]:[DATASET].[TABLE]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.bigquery._
import com.spotify.scio.ContextAndArgs

object TypedStorageBigQueryTornadoes {
  // Annotate input class with schema inferred.
  // Class `Row` will be expanded into a case class with selected fields. A companion
  // object will also be generated to provide easy access to original table from annotation,
  // `TableSchema` and converter methods between the generated case class and `TableRow`.
  @BigQueryType.fromStorage(
    "bigquery-public-data:samples.gsod",
    selectedFields = List("tornado", "month"),
    rowRestriction = "tornado = true"
  )
  class Row

  // Annotate output case class.
  // Note that the case class is already defined and will not be expanded. Only the companion
  // object will be generated to provide easy access to `TableSchema` and converter methods.
  @BigQueryType.toTable
  case class Result(month: Long, tornado_count: Long)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Get input from BigQuery and convert elements from `TableRow` to `Row`.
    // SELECT query from the original annotation is used by default.
    sc.typedBigQuery[Row]()
      .map(_.month)
      .countByValue
      .map(kv => Result(kv._1, kv._2))
      // Convert elements from Result to TableRow and save output to BigQuery.
      .saveAsTypedBigQueryTable(
        Table.Spec(args("output")),
        writeDisposition = WRITE_TRUNCATE,
        createDisposition = CREATE_IF_NEEDED
      )

    sc.run().waitUntilDone()
    ()
  }
}
