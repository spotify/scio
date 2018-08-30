/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.{io => beam}

// Example: Word Count Example writing to TSV file
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.TsvExampleWrite
//   --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
//   --input=gs://apache-beam-samples/shakespeare/kinglear.txt
//   --output=gs://[BUCKET]/[PATH]/wordcount"`
object TsvExampleWrite {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Parse input and output path from command line arguments
    val input = args.getOrElse("input", ExampleData.KING_LEAR)
    val output = args("output")

    // Create a Write Transformation with tsv suffix, single file and specific header.
    val transform = beam.TextIO.write()
      .to(pathWithShards(output))
      .withSuffix(".tsv")
      .withNumShards(1)
      .withHeader("word\tcount")

    // Open text files as an `SCollection[String]`
    sc.textFile(input)
      // Split input lines, filter out empty tokens and expand into a collection of tokens
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // Count occurrences of each unique `String` to get `(String, Long)`
      .countByValue
      // Map `(String, Long)` tuples into strings
      .map(t => t._1 + "\t" + t._2)
      // Save result as a text files under the output path
      .saveAsCustomOutput(output, transform)

    // Close the context
    sc.close()
  }

  // Shard output filename generator function. See `TypedWrite.to`.
  private def pathWithShards(path: String) =
    path.replaceAll("\\/+$", "") + "/part"
}

// # Reading TSV data Example
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.TsvExampleRead
//   --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
//   --input=gs://[BUCKET]/word_count_data.tsv
//   --output=gs://[BUCKET]/[PATH]/word_count_sum"`
object TsvExampleRead {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Open text files as an `SCollection[String]`
    sc.textFile(args("input"))
      // Filter out the header
      .filter(!_.contains("word\tcount"))
      // Parse TSV data: split text line into separate elements and trim whitespaces
      .map(l => l.split("\t").map(e => e.trim))
      // Parse count (second element) as `Int` to get a `SCollection[Long]`
      .map { e =>
        // Fail on invalid records
        assume(e.length == 2, "Invalid record, should be two fields separated by '\\t'")
        e(1).toLong
      }
      // Sum the content to get a single `Long` element in a `SCollection[Long]`
      .sum
      // Save result as a text files under the output path
      .saveAsTextFile(args("output"))
    // Close the context
    sc.close()
  }
}
