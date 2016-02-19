/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio.examples

import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

/*
SBT
runMain
  com.spotify.scio.examples.WordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args.getOrElse("input", ExampleData.KING_LEAR)
    val output = args.optional("output").getOrElse(
      if (sc.options.getStagingLocation != null) {
        GcsPath.fromUri(sc.options.getStagingLocation).resolve("counts.txt").toString
      } else {
        throw new IllegalArgumentException("Must specify --output or --stagingLocation")
      })

    // initialize accumulators
    val max = sc.maxAccumulator[Int]("maxLineLength")
    val min = sc.minAccumulator[Int]("minLineLength")
    val sum = sc.sumAccumulator[Long]("emptyLines")

    sc.textFile(input)
      .withAccumulator(max, min, sum)  // convert to SCollectionWithAccumulator
      // specialized version of filter with AccumulatorContext as second argument
      .filter { (l, c) =>
        val t = l.trim

        // update accumulators "max" and "min"
        c.addValue(max, t.length).addValue(min, t.length)

        val b = t.isEmpty
        if (b) c.addValue(sum, 1L) // update accumulator "sum"
        !b
      }
      .toSCollection  // convert back to normal SCollection
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(output)

    val result = sc.close()

    // wait for pipeline to complete
    while (!result.isCompleted) {
      Thread.sleep(1000)
    }

    // retrieve accumulator values
    println("Max: " + result.accumulatorTotalValue(max))
    println("Min: " + result.accumulatorTotalValue(min))
    println("Sum: " + result.accumulatorTotalValue(sum))
  }
}
