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

// Example: Calculate the score for a team over a time window

// Usage:

// `sbt runMain "com.spotify.scio.examples.complete.game.HourlyTeamScore
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --windowDuration=60
// --startMin=2018-05-13-00-00
// --stopMin=2018-05-14-00-00
// --output=bq://[PROJECT]/[DATASET]/mobile_game_hourly_team_score"`

package com.spotify.scio.examples.complete.game

import java.util.TimeZone

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.{DateTimeZone, Duration, Instant}
import org.joda.time.format.DateTimeFormat

object HourlyTeamScore {

  // The schema for the BigQuery table to write output to is defined as an annotated case class
  @BigQueryType.toTable
  case class TeamScoreSums(user: String, total_score: Int, window_start: String)

  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Date formatters for full timestamp and short timestamp
    def fmt =
      DateTimeFormat
        .forPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")))
    val minFmt = DateTimeFormat
      .forPattern("yyyy-MM-dd-HH-mm")
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")))

    // The earliest time a scoring event can be.
    // If not passed in it defaults to midnight on Jan 1 1970 (in PST)
    val startMin =
      new Instant(minFmt.parseMillis(args.getOrElse("startMin", "1970-01-01-00-00"))).getMillis
    // The latest time a scoring event can be.
    // If not passed in it defaults to midnight on Jan 1 2100 (in PST)
    val stopMin =
      new Instant(
        minFmt
          .parseMillis(args.getOrElse("stopMin", "2100-01-01-00-00"))
      ).getMillis
    // Minutes to group events by - defaults to 60 minutes if not passed in
    val windowDuration = args.long("windowDuration", 60L)
    // A text file containing data on events
    val input = args.getOrElse("input", ExampleData.GAMING)

    sc.textFile(input)
      // Parse each line as `GameActionInfo` events, keep the ones that successfully parsed
      .flatMap(UserScore.parseEvent)
      // Filter out events before start time and after end time
      .filter(i => i.timestamp > startMin && i.timestamp < stopMin)
      // Mark each event in the SCollection with the timestamp of the event
      .timestampBy(i => new Instant(i.timestamp))
      // Window by the number of minutes in the window duration
      .withFixedWindows(Duration.standardMinutes(windowDuration))
      // Change each event into a tuple of: team user was on, and that user's score
      .map(i => (i.team, i.score))
      // Sum the scores across the defined window, using "team" as the key to sum by
      .sumByKey
      .withWindow[IntervalWindow]
      // Map summed results from tuples into `TeamScoreSums` case class, so we can save to BQ
      .map {
        case ((team, score), window) =>
          val start = fmt.print(window.start())
          TeamScoreSums(team, score, start)
      }
      // Save to the BigQuery table defined by "output" in the arguments passed in
      .saveAsTypedBigQuery(args("output"))

    // Close context and execute the pipeline
    sc.close()
    ()
  }

}
