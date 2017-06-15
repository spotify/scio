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

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.metrics.ScioMetric

/*
SBT
runMain
  scio-examples/runMain com.spotify.scio.examples.WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args.getOrElse("input", ExampleData.KING_LEAR)
    val output = args("output")

    val lineDist = ScioMetric.distribution("lineLength")
    val sumNonEmpty = ScioMetric.counter("nonEmptyLines")
    val sumEmpty = ScioMetric.counter("emptyLines")

    sc.textFile(input)
      .map { w =>
        val trimmed = w.trim
        val l = trimmed.length
        lineDist.update(l)
        trimmed
      }
      .filter { w =>
        val r = w.nonEmpty
        if (r) {
          sumNonEmpty.inc()
        }
        else {
          sumEmpty.inc()
        }
        r
      }
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(output)

    val result = sc.close().waitUntilFinish()

    // scalastyle:off regex
    // retrieve accumulator values
    val counters = result.getCounters()
    val distributions = result.getDistributions()
    println("Max: " + distributions("lineLength").committed.get.max)
    println("Min: " + distributions("lineLength").committed.get.min)
    println("Sum non-empty: " + counters("nonEmptyLines").committed.get)
    println("Sum empty: " + counters("emptyLines").committed.get)
    // scalastyle:on regex
  }
}
