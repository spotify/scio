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

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio._
import com.spotify.scio.accumulators._
import com.spotify.scio.examples.common.ExampleData

/*
sbt "scio-examples/runMain com.spotify.scio.examples.WordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val dfOptions = sc.optionsAs[DataflowPipelineOptions]

    val input = args.getOrElse("input", ExampleData.KING_LEAR)
    val output = args.optional("output").getOrElse(
      if (dfOptions.getStagingLocation != null) {
        GcsPath.fromUri(dfOptions.getStagingLocation).resolve("counts.txt").toString
      } else {
        throw new IllegalArgumentException("Must specify --output or --stagingLocation")
      })

    // initialize accumulators
    val max = sc.maxAccumulator[Int]("maxLineLength")
    val min = sc.minAccumulator[Int]("minLineLength")
    val sumNonEmpty = sc.sumAccumulator[Long]("nonEmptyLines")
    val sumEmpty = sc.sumAccumulator[Long]("emptyLines")

    sc.textFile(input)
      .map(_.trim)
      .accumulateBy(max, min)(_.length)
      .accumulateCountFilter(sumNonEmpty, sumEmpty)(_.nonEmpty)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(output)

    val result = sc.close()

    // wait for pipeline to complete
    while (!result.isCompleted) {
      Thread.sleep(1000)
    }

    // scalastyle:off regex
    // retrieve accumulator values
    println("Max: " + result.accumulatorTotalValue(max))
    println("Min: " + result.accumulatorTotalValue(min))
    println("Sum non-empty: " + result.accumulatorTotalValue(sumNonEmpty))
    println("Sum empty: " + result.accumulatorTotalValue(sumEmpty))
    // scalastyle:on regex
  }
}
