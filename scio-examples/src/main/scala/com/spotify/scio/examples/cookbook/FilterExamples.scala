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

// Example: Filter Example
// Usage:

// `sbt runMain "com.spotify.scio.examples.cookbook.FilterExamples
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --output=[DATASET].filter_examples"`
package com.spotify.scio.examples.cookbook

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

import scala.collection.JavaConverters._

// Intermediate record type
case class Record(year: Long, month: Long, day: Long, meanTemp: Double)

object FilterExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Schema for result BigQuery table
    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("year").setType("INTEGER"),
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("day").setType("INTEGER"),
      new TableFieldSchema().setName("mean_temp").setType("FLOAT")
    ).asJava)

    val monthFilter = args.int("monthFilter", 7)

    // Open BigQuery table as a `SCollection[TableRow]`
    val pipe = sc.bigQueryTable(args.getOrElse("input", ExampleData.WEATHER_SAMPLES_TABLE))
      // Map `TableRow`s into `Record`s
      .map { row =>
        val year = row.getLong("year")
        val month = row.getLong("month")
        val day = row.getLong("day")
        val meanTemp = row.getDouble("mean_temp")
        Record(year, month, day, meanTemp)
      }

    // Compute mean temperature as a `SCollection[Double]` of a single value
    val globalMeanTemp = pipe.map(_.meanTemp).mean

    pipe
      // Filter by month
      .filter(_.month == monthFilter)
      // Cross product of elements in the two `SCollection`s, and since the right hand side is a
      // singleton, effectively decorate each `Record` as `(Record, Double)`
      .cross(globalMeanTemp)
      // Filter by mean temperature
      .filter(kv => kv._1.meanTemp < kv._2)
      // Keep the keys (`Record`) and iscard the values (`Double`)
      .keys
      // Map `Record`s into result `TableRow`s
      .map { r =>
        TableRow("year" -> r.year, "month" -> r.month, "day" -> r.day, "mean_temp" -> r.meanTemp)
      }
      // Save result as a BigQuery table
      .saveAsBigQuery(args("output"), schema, WRITE_TRUNCATE, CREATE_IF_NEEDED)

    // Close the context and execute the pipeline
    sc.close()
  }
}
