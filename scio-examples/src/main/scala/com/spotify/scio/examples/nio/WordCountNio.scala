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

// Example: Word Count Example with Metrics and nio read/write
// Usage:

// `sbt runMain "com.spotify.scio.examples.nio.WordCountNio
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/wordcount"`
package com.spotify.scio.examples.nio

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.nio
import org.slf4j.LoggerFactory

object WordCountNio {

  // Logger is an object instance, i.e. statically initialized and thus can be used safely in an
  // anonymous function without serialization issue
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Parse input and output path from command line arguments
    val input = args.getOrElse("input", ExampleData.KING_LEAR)
    val output = args("output")

    // Create a distribution and two counter metrics. `Distribution` tracks min, max, sum, min,
    // etc. and `Counter` tracks count.
    val lineDist = ScioMetrics.distribution("lineLength")
    val sumNonEmpty = ScioMetrics.counter("nonEmptyLines")
    val sumEmpty = ScioMetrics.counter("emptyLines")

    // Create IO classes to read and write
    val inputTextIO = nio.TextIO(input)
    val outputTextIO = nio.TextIO(output)

    // Open text files as an `SCollection[String]` passing io read params
    sc.read(inputTextIO)(inputTextIO.ReadParams())
      .map { w =>
        // Trim input lines, update distribution metric
        val trimmed = w.trim
        lineDist.update(trimmed.length)
        trimmed
      }
      .filter { w =>
        // Filter out empty lines, update counter metrics
        val r = w.nonEmpty
        if (r) sumNonEmpty.inc() else sumEmpty.inc()
        r
      }
      // Split input lines, filter out empty tokens and expand into a collection of tokens
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // Count occurrences of each unique `String` to get `(String, Long)`
      .countByValue
      // Map `(String, Long)` tuples into final "word: count" strings
      .map { case (word, count) => word + ": " + count }
      // Save result as text files under the output path by passing write params
      .write(outputTextIO)(outputTextIO.WriteParams())

    // Close the context, execute the pipeline and block until it finishes
    val result = sc.close().waitUntilFinish()

    // Retrieve metric values
    logger.info("Max: " + result.distribution(lineDist).committed.map(_.getMax))
    logger.info("Min: " + result.distribution(lineDist).committed.map(_.getMin))
    logger.info("Sum non-empty: " + result.counter(sumNonEmpty).committed)
    logger.info("Sum empty: " + result.counter(sumEmpty).committed)
  }
}
