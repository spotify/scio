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

// Example: Word Count Example with Assertions
// Usage:

// `sbt runMain "com.spotify.scio.examples.DebuggingWordCount
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/wordcount"`
package com.spotify.scio.examples

import java.util.regex.Pattern

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.testing.PAssert
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object DebuggingWordCount {

  // Logger is an object instance, i.e. statically initialized and thus can be used safely in an
  // anonymous function without serialization issue
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val filter = Pattern.compile(args.getOrElse("filterPattern", "Flourish|stomach"))

    // Create two counter metrics
    val matchedWords = ScioMetrics.counter("matchedWords")
    val unmatchedWords = ScioMetrics.counter("unmatchedWords")

    val filteredWords = sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      // Split input lines, filter out empty tokens and expand into a collection of tokens
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // Count occurrences of each unique `String` to get `(String, Long)`
      .countByValue
      // Filter out tokens that matches the pattern, log, and increment counters
      .filter { case (k, _) =>
        val matched = filter.matcher(k).matches()
        if (matched) {
          logger.debug(s"Matched $k")
          matchedWords.inc()
        } else {
          logger.trace(s"Did not match: $k")
          unmatchedWords.inc()
        }
        matched
      }

    // Verify internal Beam `PCollection` with `PAssert`
    PAssert.that(filteredWords.internal)
      .containsInAnyOrder(List(("Flourish", 3L), ("stomach", 1L)).asJava)

    // Close the context, execute the pipeline and block until it finishes
    val result = sc.close().waitUntilFinish()

    // Retrieve metric values
    require(result.counter(matchedWords).committed.get == 2)
    require(result.counter(unmatchedWords).committed.get > 100)
  }

}
