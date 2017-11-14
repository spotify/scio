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

// Example: Combine Per Key Example
// Usage:

// `sbt runMain "com.spotify.scio.examples.cookbook.CombinePerKeyExamples
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --output=[DATASET].combine_per_key_examples"`
package com.spotify.scio.examples.cookbook

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

import scala.collection.JavaConverters._
import scala.collection.SortedSet

object CombinePerKeyExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val minWordLength = 9

    // Schema for result BigQuery table
    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING"),
      new TableFieldSchema().setName("all_plays").setType("STRING")
    ).asJava)

    // Open a BigQuery table as a `SCollection[TableRow]`
    sc.bigQueryTable(args.getOrElse("input", ExampleData.SHAKESPEARE_TABLE))
      // Extract words and corresponding play names
      .flatMap { row =>
        val playName = row.getString("corpus")
        val word = row.getString("word")
        if (word.length > minWordLength) Some((word, playName)) else None
      }
      // Aggregate values (play name) of the same key (word), starting with an initial result
      // (empty set), first accumulate values into the set (`_ + _`), then combine the intermediate
      // results (`_ ++ _`). Sort values in a set to make test happy.
      .aggregateByKey(SortedSet[String]())(_ + _, _ ++ _)
      // Map result sets into strings
      .mapValues(_.mkString(","))
      // Map `(String, String)` tuples into result `TableRow`s
      .map(kv => TableRow("word" -> kv._1, "all_plays" -> kv._2))
      // Save result as a BigQuery table
      .saveAsBigQuery(args("output"), schema, WRITE_TRUNCATE, CREATE_IF_NEEDED)

    // Close the context and execute the pipeline
    sc.close()
  }
}
