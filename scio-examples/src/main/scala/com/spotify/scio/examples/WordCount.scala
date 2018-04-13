/*
 * Copyright 2018 Spotify AB.
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

// Example: Word Count Example with Metrics
// Usage:

// `sbt runMain "com.spotify.scio.examples.WordCount
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/wordcount"`
package com.spotify.scio.examples

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import org.slf4j.LoggerFactory

object WordCount {

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

    // Open text files as an `SCollection[String]`
    sc.textFile(input)
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
      // Map `(String, Long)` tuples into strings
      .map(t => t._1 + ": " + t._2)
      // Save result as text files under the output path
      .saveAsTextFile(output)

    // Close the context, execute the pipeline and block until it finishes
    val result = sc.close().waitUntilFinish()

    // Retrieve metric values
    logger.info("Max: " + result.distribution(lineDist).committed.map(_.max()))
    logger.info("Min: " + result.distribution(lineDist).committed.map(_.min()))
    logger.info("Sum non-empty: " + result.counter(sumNonEmpty).committed)
    logger.info("Sum empty: " + result.counter(sumEmpty).committed)
  }
}
