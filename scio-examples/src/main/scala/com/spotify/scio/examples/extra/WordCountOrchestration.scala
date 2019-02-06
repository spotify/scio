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

// Example: Use Futures and Taps to orchestrate multiple jobs with dependencies
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.WordCountOrchestration
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --output=gs://[BUCKET]/[PATH]/wordcount"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.options.PipelineOptions

import scala.concurrent.Future

object WordCountOrchestration {

  def main(cmdlineArgs: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val (opts, args) = ScioContext.parseArguments[PipelineOptions](cmdlineArgs)

    val output = args("output")

    // Submit count job 1
    val f1 = Future(count(opts, ExampleData.KING_LEAR))

    // Submit count job 2
    val f2 = Future(count(opts, ExampleData.OTHELLO))

    import scala.concurrent.ExecutionContext.Implicits.global

    // extract `Tap[T]`s from two `Future[Tap[T]]`s
    val f = for {
      t1 <- f1
      t2 <- f2
    } yield merge(opts, Seq(t1, t2), output)

    // scalastyle:off regex
    // Block process and wait for last future
    println("Tap:")
    f.value.take(10).foreach(println)
    // scalastyle:on regex
  }

  def count(opts: PipelineOptions, inputPath: String): Tap[(String, Long)] = {
    val sc = ScioContext(opts)
    val f = sc
      .textFile(inputPath)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .materialize
    sc.close().waitUntilDone().tap(f)
  }

  // Split out transform for unit testing
  def countWords(in: SCollection[String]): SCollection[(String, Long)] =
    in.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty)).countByValue

  def merge(opts: PipelineOptions, s: Seq[Tap[(String, Long)]], outputPath: String): Tap[String] = {
    val sc = ScioContext(opts)
    val f = mergeCounts(s.map(_.open(sc)))
      .map(kv => kv._1 + " " + kv._2)
      .saveAsTextFile(outputPath)

    sc.close().waitUntilDone().tap(f)
  }

  // Split out transform for unit testing
  def mergeCounts(ins: Seq[SCollection[(String, Long)]]): SCollection[(String, Long)] =
    SCollection.unionAll(ins).sumByKey

}
