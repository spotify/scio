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

package com.spotify.scio.examples

import java.util.regex.Pattern

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.testing.PAssert
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.DebuggingWordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://apache-beam-samples/shakespeare/kinglear.txt
*/

object DebuggingWordCount {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val filter = Pattern.compile(args.getOrElse("filterPattern", "Flourish|stomach"))

    val matchedWords = ScioMetrics.counter("matchedWords")
    val unmatchedWords = ScioMetrics.counter("unmatchedWords")

    val filteredWords = sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
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

    // verify internal PCollection
    PAssert.that(filteredWords.internal)
      .containsInAnyOrder(List(("Flourish", 3L), ("stomach", 1L)).asJava)

    val result = sc.close().waitUntilFinish()

    // retrieve metric values
    require(result.counter(matchedWords).committed.get == 2)
    require(result.counter(unmatchedWords).committed.get > 100)
  }

}
