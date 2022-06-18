/*
 * Contributed by Siddharth Jain <sijain2@cisco.com>
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

// Example: Select top N records from a table in BigQuery
// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.Natality
// --project=[PROJECT] --runner=DataflowRunner --region=[ZONE]
// --output=[PROJECT]:[DATASET].[TABLE]"`

package com.spotify.scio.examples.extra

import com.spotify.scio.bigquery._
import com.spotify.scio.{ContextAndArgs, ScioContext}

object Natality {
  // Annotate input class with schema inferred from a BigQuery SELECT.
  // Class `Row` will be expanded into a case class with fields from the SELECT query. A companion
  // object will also be generated to provide easy access to original query/table from annotation,
  // `TableSchema` and converter methods between the generated case class and `TableRow`.
  @BigQueryType.fromQuery("SELECT weight_pounds FROM [bigquery-public-data:samples.natality]")
  class Row

  // Annotate output case class.
  // Note that the case class is already defined and will not be expanded. Only the companion
  // object will be generated to provide easy access to `TableSchema` and converter methods.
  @BigQueryType.toTable
  case class Result(weight_pounds: Double)

  def pipeline(cmdlineArgs: Array[String]): ScioContext = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Get input from BigQuery and convert elements from `TableRow` to `Row`.
    // SELECT query from the original annotation is used by default.
    sc.typedBigQuery[Row]()
      .map(r => r.weight_pounds.getOrElse(0.0)) // return 0 if weight_pounds is None
      .top(100) // select top 100. this returns a SCollection[Iterable[Double]]
      .flatten
      .map(x => Result(x))
      // Convert elements from Result to TableRow and save output to BigQuery.
      .saveAsTypedBigQueryTable(
        Table.Spec(args("output")),
        writeDisposition = WRITE_TRUNCATE,
        createDisposition = CREATE_IF_NEEDED
      )

    sc
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val sc = pipeline(cmdlineArgs)
    sc.run().waitUntilDone()
    ()
  }
}
