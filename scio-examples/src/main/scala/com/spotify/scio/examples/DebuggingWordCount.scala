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

import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.spotify.scio._
import com.spotify.scio.accumulators._
import com.spotify.scio.examples.common.ExampleData

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.DebuggingWordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
*/

object DebuggingWordCount {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val matchedWords = sc.sumAccumulator[Long]("matchedWords")
    val unmatchedWords = sc.sumAccumulator[Long]("unmatchedWords")

    val filteredWords = sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .accumulateCountFilter(matchedWords, unmatchedWords) { kv =>
        Set("Flourish", "stomach").contains(kv._1)
      }

    // verify internal PCollection
    DataflowAssert.that(filteredWords.internal)
      .containsInAnyOrder(List(("Flourish", 3L), ("stomach", 1L)).asJava)

    val result = sc.close()

    // wait for pipeline to complete
    while (!result.isCompleted) {
      Thread.sleep(1000)
    }

    // retrieve accumulator values
    require(result.accumulatorTotalValue(matchedWords) == 2)
    require(result.accumulatorTotalValue(unmatchedWords) > 100)
  }

}
