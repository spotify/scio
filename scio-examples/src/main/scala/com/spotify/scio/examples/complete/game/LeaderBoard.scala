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

// Example: Calculate leaderboard for game (highest team scores, highest user scores)

// Usage:

// `sbt runMain "com.spotify.scio.examples.complete.game.LeaderBoard
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --teamWindowDuration=60
// --allowedLateness=120
// --topic=[PUBSUB_TOPIC_NAME]
// --output=bq://[PROJECT]/[DATASET]/mobile_game"`

package com.spotify.scio.examples.complete.game

import java.util.TimeZone

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.complete.game.UserScore.GameActionInfo
import com.spotify.scio.streaming._
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, Duration, Instant}

object LeaderBoard {

  // The schemas for the BigQuery tables to write output to are defined as annotated case classes
  @BigQueryType.toTable
  case class TeamScoreSums(team: String,
                           total_score: Int,
                           window_start: String,
                           processing_time: String,
                           timing: String)

  @BigQueryType.toTable
  case class UserScoreSums(user: String, total_score: Int, processing_time: String)

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    sc.optionsAs[StreamingOptions].setStreaming(true)
    val exampleUtils = new ExampleUtils(sc.options)

    // Date formatter for full timestamp
    def fmt =
      DateTimeFormat
        .forPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")))
    // Duration in minutes over which to calculate team scores, defaults to 1 hour
    val teamWindowDuration =
      Duration.standardMinutes(args.int("teamWindowDuration", 60))
    // Data that comes in from our streaming pipeline after this duration isn't considered in our
    // processing. Measured in minutes, defaults to 2 hours
    val allowedLateness =
      Duration.standardMinutes(args.int("allowedLateness", 120))

    // Read in streaming data from PubSub and parse each row as `GameActionInfo` events
    val gameEvents = sc
      .pubsubTopic[String](args("topic"), timestampAttribute = "timestamp_ms")
      .flatMap(UserScore.parseEvent)

    calculateTeamScores(gameEvents, teamWindowDuration, allowedLateness)
    // Add windowing information to team score results by converting to `WindowedSCollection`
    .toWindowed
      .map { wv =>
        // Convert from score tuple to `TeamScoreSums` case class with both tuple and windowing info
        val start = fmt.print(wv.window.asInstanceOf[IntervalWindow].start())
        val now = fmt.print(Instant.now())
        val timing = wv.pane.getTiming.toString
        wv.copy(value = TeamScoreSums(wv.value._1, wv.value._2, start, now, timing))
      }
      // Done with windowing information, convert back to regular `SCollection`
      .toSCollection
      // Save to the BigQuery table defined by "output" in the arguments passed in + "_team" suffix
      .saveAsTypedBigQuery(args("output") + "_team")

    gameEvents
    // Use a global window for unbounded data, which updates calculation every 10 minutes,
    // starting 10 minutes after the first event arrives.
    // Recalculates immediately when late data arrives, according to these rules:
    // - Accumulates fired panes (rather than discarding), which means subsequent calculations
    // use all data, not just data collected after the last calculation was done.
    // - Accepts late entries (and recalculates based on them) only if they arrive within the
    // allowedLateness duration.
    // For more information on these options, see the Beam docs:
    // https://beam.apache.org/documentation/programming-guide/#triggers
      .withGlobalWindow(WindowOptions(
        trigger = Repeatedly.forever(AfterProcessingTime
          .pastFirstElementInPane()
          .plusDelayOf(Duration.standardMinutes(10))),
        accumulationMode = ACCUMULATING_FIRED_PANES,
        allowedLateness = allowedLateness
      ))
      // Change each event into a tuple of: user, and that user's score
      .map(i => (i.user, i.score))
      // Sum the scores by user
      .sumByKey
      // Map summed results from tuples into `UserScoreSums` case class, so we can save to BQ
      .map(kv => UserScoreSums(kv._1, kv._2, fmt.print(Instant.now())))
      // Save to the BigQuery table defined by "output" in the arguments passed in + "_user" suffix
      .saveAsTypedBigQuery(args("output") + "_user")

    // Close context and execute the pipeline
    val result = sc.close()
    // Wait to finish processing before exiting when streaming pipeline is canceled during shutdown
    exampleUtils.waitToFinish(result.pipelineResult)
  }
  // scalastyle:on method.length

  def calculateTeamScores(infos: SCollection[GameActionInfo],
                          teamWindowDuration: Duration,
                          allowedLateness: Duration): SCollection[(String, Int)] =
    infos
      .withFixedWindows(
        // Using a fixed window, calculate every time the window ends.
        // Also calculate an early/"speculative" result from partial data, 5 minutes after the first
        // element in our window is processed (withEarlyFirings).
        // If late data arrives, 10 minutes after the late data arrives, recalculate according to:
        // - Accumulates fired panes (rather than discarding), which means subsequent calculations
        // use all data, not just data collected after the last calculation was done.
        // - Accepts late entries (and recalculates based on them) only if they arrive within the
        // allowedLateness duration.
        // For more information on these options, see the Beam
        // [doc](https://beam.apache.org/documentation/programming-guide/#composite-triggers)
        // (subsection 8.5.2, "Composition with AfterWatermark", is especially relevant)
        teamWindowDuration,
        options = WindowOptions(
          trigger = AfterWatermark
            .pastEndOfWindow()
            .withEarlyFirings(
              AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(5)))
            .withLateFirings(AfterProcessingTime
              .pastFirstElementInPane()
              .plusDelayOf(Duration.standardMinutes(10))),
          accumulationMode = ACCUMULATING_FIRED_PANES,
          allowedLateness = allowedLateness
        )
      )
      // Change each event into a tuple of: team user was on, and that user's score
      .map(i => (i.team, i.score))
      // Sum the scores across the defined window, using "team" as the key to sum by
      .sumByKey

}
