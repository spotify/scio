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

// Example: Max Per Key Example
// Usage:

// `sbt runMain "com.spotify.scio.examples.cookbook.MaxPerKeyExamples
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --output=[DATASET].max_per_key_examples"`
package com.spotify.scio.examples.cookbook

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

import scala.collection.JavaConverters._

object MaxPerKeyExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Schema for result BigQuery table
    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("max_mean_temp").setType("FLOAT")).asJava)

    // Open a BigQuery table as a `SCollection[TableRow]`
    sc.bigQueryTable(args.getOrElse("input", ExampleData.WEATHER_SAMPLES_TABLE))
      // Extract month and mean temperature as `(Long, Double)` tuples
      .map(row => (row.getLong("month"), row.getDouble("mean_temp")))
      // For multiple values (mean temperatures) of the same key (month), compute the maximum per
      // key
      .maxByKey
      // Map `(Long, Double)` tuples into result `TableRow`s
      .map(kv => TableRow("month" -> kv._1, "max_mean_temp" -> kv._2))
      // Save result as a BigQuery table
      .saveAsBigQuery(args("output"), schema, WRITE_TRUNCATE, CREATE_IF_NEEDED)

    // Close the context and execute the pipeline
    sc.close()
  }
}
