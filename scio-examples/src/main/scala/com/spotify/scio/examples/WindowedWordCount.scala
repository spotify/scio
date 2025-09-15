/*
 * Copyright 2019 Spotify AB.
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

// Example: Word Count Example with Windowing
// Usage:

// `sbt "runMain com.spotify.scio.examples.WindowedWordCount
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/wordcount"`
package com.spotify.scio.examples

import java.nio.channels.Channels
import java.util.concurrent.ThreadLocalRandom

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.streaming.DISCARDING_FIRED_PANES
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.transforms.windowing.{
  AfterWatermark,
  GlobalWindow,
  IntervalWindow,
  Repeatedly
}
import org.apache.beam.sdk.util.MimeTypes
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{Duration, Instant}

object WindowedWordCount {
  private val WINDOW_SIZE = Duration.standardMinutes(10L)
  private val formatter = ISODateTimeFormat.hourMinute

  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Initialize `FileSystem` abstraction
    FileSystems.setDefaultPipelineOptions(sc.options)

    // Parse command line arguments
    val input = args.getOrElse("input", ExampleData.KING_LEAR)
    val windowSize = Option(args("windowSize")).map(new Duration(_)).getOrElse(WINDOW_SIZE)
    val minTimestamp =
      args.long("minTimestampMillis", System.currentTimeMillis())
    val maxTimestamp =
      args.long("maxTimestampMillis", minTimestamp + Duration.standardHours(1).getMillis)

    val outputGlobalWindow = args.boolean("outputGlobalWindow", false)

    // Open text files as an `SCollection[String]`
    val wordCounts = sc
      .textFile(input)
      .transform("random timestamper") {
        // Assign random timestamps to each element
        _.timestampBy { _ =>
          new Instant(ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp))
        }
      }
      .transform("windowed counter") {
        // Apply windowing logic
        _.withFixedWindows(windowSize)
          // Split input lines, filter out empty tokens and expand into a collection of tokens
          .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
          // Count occurrences of each unique `String` within each window to get `(String, Long)`
          .countByValue
          // Expose window information as an `IntervalWindow`
          .withWindow[IntervalWindow]
          // Swap keys and values, i.e. `((String, Long), IntervalWindow)` => `(IntervalWindow,
          // (String, Long))`
          .swap
          // Group elements by window to get `(IntervalWindow, Iterable[(String, Long)])
          .groupByKey
      }

    if (outputGlobalWindow) {
      wordCounts
        .withGlobalWindow(
          WindowOptions(
            allowedLateness = Duration.ZERO,
            trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
            accumulationMode = DISCARDING_FIRED_PANES
          )
        )
        .withWindow[GlobalWindow]
        .flatMap { case ((_, counts), _) => counts }
        .saveAsTextFile("output.txt")
    } else {
      // Write values in each group to a separate text file
      wordCounts.map { case (w, vs) =>
        val outputShard =
          "%s-%s-%s".format(args("output"), formatter.print(w.start()), formatter.print(w.end()))
        val resourceId = FileSystems.matchNewResource(outputShard, false)
        val out = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.TEXT))
        vs.foreach { case (k, v) => out.write(s"$k: $v\n".getBytes) }
        out.close()
      }
    }

    // Execute the pipeline
    sc.run()
    ()
  }
}
