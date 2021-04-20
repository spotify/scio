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
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
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
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val samplingThreshold = 0.1

    val input = sc.tableRowJsonFile(args.getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
    computeTopSessions(input, samplingThreshold).saveAsTextFile(args("output"))

    sc.run()
    ()
  }

  def computeTopSessions(
    input: SCollection[TableRow],
    samplingThreshold: Double
  ): SCollection[String] =
    input
      // Extract fields from `TableRow` JSON
      .flatMap { row =>
        val username = row.getString("contributor_username")
        val timestamp = row.getLong("timestamp")
        if (username == null) {
          None
        } else {
          Some((username, timestamp))
        }
      }
      // Assign timestamp to each element
      .timestampBy(kv => new Instant(kv._2 * 1000L))
      // Drop field now that elements are timestamped
      .map(_._1)
      .sample(withReplacement = false, fraction = samplingThreshold)
      // Apply session windows on a per-key (username) bases
      .withSessionWindows(Duration.standardHours(1))
      // Count number of edits per user per window
      .countByValue
      // Convert to a `WindowedSCollection` to expose window information
      .toWindowed
      // Concatenate window information to username
      .map(wv => wv.copy((wv.value._1 + " : " + wv.window, wv.value._2)))
      // End of windowed operation, convert back to a regular `SCollection`
      .toSCollection
      // Apply fixed windows
      .windowByMonths(1)
      // Compute top `(username, count)` per month
      .top(1)(Ordering.by(_._2))
      // Convert to a `WindowedSCollection` to expose window information
      .toWindowed
      .flatMap { wv =>
        wv.value.map { kv =>
          // Format output with username, count and window start timestamp
          val o = kv._1 + " : " + kv._2 + " : " + wv.window
            .asInstanceOf[IntervalWindow]
            .start()
          wv.copy(value = o)
        }
      }
      // End of windowed operation, convert back to a regular `SCollection`
      .toSCollection
}
