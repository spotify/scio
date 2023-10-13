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

// Example: Top Wikipedia Sessions
// Usage:

// `sbt "runMain com.spotify.scio.examples.complete.TopWikipediaSessions
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --input=gs://apache-beam-samples/wikipedia_edits/wiki_data-*.json
// --output=gs://[BUCKET]/[PATH]/top_wikipedia_sessions"`
package com.spotify.scio.examples.complete

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.{Duration, Instant}

object TopWikipediaSessions {

  val SamplingThreshold = 0.1

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = sc.tableRowJsonFile(args.getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
    computeTopSessions(input, SamplingThreshold).saveAsTextFile(args("output"))

    sc.run()
  }

  def computeTopSessions(
    input: SCollection[TableRow],
    samplingThreshold: Double
  ): SCollection[String] =
    input
      // Extract fields from `TableRow` JSON
      .flatMap { row =>
        for {
          username <- Option(row.getString("contributor_username"))
          timestamp <- Option(row.getLong("timestamp"))
        } yield username -> timestamp
      }
      // Assign timestamp to each element
      .timestampBy { case (_, ts) => new Instant(ts * 1000L) }
      // Drop field now that elements are timestamped
      .keys
      .sample(withReplacement = false, fraction = samplingThreshold)
      // Apply session windows on a per-key (username) bases
      .withSessionWindows(Duration.standardHours(1))
      // Count number of edits per user per window
      .countByValue
      // Convert to a `WindowedSCollection` to expose window information
      .toWindowed
      // Concatenate window information to username
      .map { wv =>
        val window = wv.window
        val (username, count) = wv.value
        wv.copy(s"$username : $window" -> count)
      }
      // End of windowed operation, convert back to a regular `SCollection`
      .toSCollection
      // Apply fixed windows
      .windowByMonths(1)
      // Compute top `(username, count)` per month
      .top(1)(Ordering.by(_._2))
      // Convert to a `WindowedSCollection` to expose window information
      .toWindowed
      .flatMap { wv =>
        val start = wv.window.asInstanceOf[IntervalWindow].start()
        wv.value.map { case (id, count) =>
          // Format output with username, count and window start timestamp
          wv.copy(value = s"$id : $count : $start")
        }
      }
      // End of windowed operation, convert back to a regular `SCollection`
      .toSCollection
}
