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

// Example: Word Count Example with Windowing
// Usage:

// `sbt runMain "com.spotify.scio.examples.WindowedWordCount
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/wordcount"`
package com.spotify.scio.examples

import java.nio.channels.Channels
import java.util.concurrent.ThreadLocalRandom

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.util.MimeTypes
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{Duration, Instant}

object WindowedWordCount {

  private val WINDOW_SIZE = 10L
  private val formatter = ISODateTimeFormat.hourMinute

  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Initialize `FileSystem` abstraction
    FileSystems.setDefaultPipelineOptions(sc.options)

    // Parse command line arguments
    val input = args.getOrElse("input", ExampleData.KING_LEAR)
    val windowSize = Duration.standardMinutes(args.long("windowSize", WINDOW_SIZE))
    val minTimestamp = args.long("minTimestampMillis", System.currentTimeMillis())
    val maxTimestamp = args.long(
      "maxTimestampMillis", minTimestamp + Duration.standardHours(1).getMillis)

    // Open text files a `SCollection[String]`
    sc.textFile(input)
      // Assign random timestamps to each element
      .timestampBy {
        _ => new Instant(ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp))
      }
      // Apply windowing logic
      .withFixedWindows(windowSize)
      // Split input lines, filter out empty tokens and expand into a collection of tokens
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // Count occurrences of each unique `String` within each window to get `(String, Long)`
      .countByValue
      // Expose window infomation as `IntervalWindow`
      .withWindow[IntervalWindow]
      // Swap keys and values, i.e. `((String, Long), IntervalWindow)` => `(IntervalWindow,
      // (String, Long))`
      .swap
      // Group elements by window to get `(IntervalWindow, Iterable[(String, Long)])
      .groupByKey
      // Write values in each group to a separate text file
      .map { case (w, vs) =>
        val outputShard = "%s-%s-%s".format(
          args("output"), formatter.print(w.start()), formatter.print(w.end()))
        val resourceId = FileSystems.matchNewResource(outputShard, false)
        val out = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.TEXT))
        vs.foreach { case (k, v) => out.write(s"$k: $v\n".getBytes) }
        out.close()
      }

    // Close the context and execute the pipeline
    sc.close()
  }

}
