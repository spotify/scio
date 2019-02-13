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

// Example: Calculate game statistics: sum of team's scores, average user session length

// Usage:

// `sbt runMain "com.spotify.scio.examples.complete.game.GameStats
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --fixedWindowDuration=60
// --sessionGap=5
// --userActivityWindowDuration=30
// --topic=[PUBSUB_TOPIC_NAME]
// --output=bq://[PROJECT]/[DATASET]/mobile_game"`

package com.spotify.scio.examples.complete.game

import java.util.TimeZone

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.{IntervalWindow, TimestampCombiner}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, Duration, Instant}

object GameStats {

  // The schemas for the BigQuery tables to write output to are defined as annotated case classes
  @BigQueryType.toTable
  case class TeamScoreSums(team: String,
                           total_score: Int,
                           window_start: String,
                           processing_time: String)
  @BigQueryType.toTable
  case class AvgSessionLength(mean_duration: Double, window_start: String)

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
    // Duration in minutes for windowing of user and team score sums, defaults to 1 hour
    val fixedWindowDuration = args.int("fixedWindowDuration", 60)
    // Duration in minutes for length of inactivity after which to start a session, defaults to 5m
    val sessionGap = args.int("sessionGap", 5)
    // Duration in minutes for windowing of user activities, defaults to 30 min
    val userActivityWindowDuration = args.int("userActivityWindowDuration", 30)

    // Read streaming events from PubSub topic, using ms of events as their ID
    val rawEvents = sc
      .pubsubTopic[String](args("topic"), idAttribute = "timestamp_ms")
      // Parse input as a `GameActionInfo` event
      .flatMap(UserScore.parseEvent)

    // Change each event into a tuple of: user, and that user's score
    val userEvents = rawEvents.map(i => (i.user, i.score))
    // Window user scores over a fixed length of time
    val userScores =
      userEvents.withFixedWindows(Duration.standardMinutes(fixedWindowDuration))
    // Calculate users with scores high enough to be anomalous, to remove from users later
    val spammyUsers = calculateSpammyUsers(userScores).asMapSideInput

    rawEvents
    // Window over a fixed length of time
      .withFixedWindows(Duration.standardMinutes(fixedWindowDuration))
      // Convert to `SCollectionWithSideInput` to use side input at same time as `SCollection` entry
      .withSideInputs(spammyUsers)
      // Filter out spammy users from this list -- `s(spammyUsers)` accesses the side input
      .filter { case (i, s) => !s(spammyUsers).contains(i.user) }
      // Done using the side input, convert back to regular `SCollection`
      .toSCollection
      // Change each event into a tuple of: team user was on, and that user's score
      .map(i => (i.team, i.score))
      // Sum the scores across the defined window, using "team" as the key to sum by
      .sumByKey
      // Convert to `WindowedSCollection` to get windowing information with values
      .toWindowed
      .map { wv =>
        // Convert windowed score to a `TeamScoreSums` object with windowing info as well as
        // team and score info
        val start = fmt.print(wv.window.asInstanceOf[IntervalWindow].start())
        val now = fmt.print(Instant.now())
        wv.copy(value = TeamScoreSums(wv.value._1, wv.value._2, start, now))
      }
      // Done using windowing information, convert back to regular `SCollection`
      .toSCollection
      // Save to the BigQuery table defined by "output" in the arguments passed in + "_team" suffix
      .saveAsTypedBigQuery(args("output") + "_team")

    userEvents
    // Window over a variable length of time - sessions end after sessionGap minutes no activity
      .withSessionWindows(Duration.standardMinutes(sessionGap),
                          options =
                            WindowOptions(timestampCombiner = TimestampCombiner.END_OF_WINDOW))
      // Get all distinct users
      .keys
      .distinct
      .withWindow[IntervalWindow]
      // Get duration of all sessions in minutes, discard user info
      .map {
        case (_, w) =>
          new Duration(w.start(), w.end())
            .toPeriod()
            .toStandardMinutes
            .getMinutes
      }
      // Find the mean value for user session length durations in a fixed time window
      .withFixedWindows(Duration.standardMinutes(userActivityWindowDuration))
      .mean
      .withWindow[IntervalWindow]
      .map {
        case (mean, w) =>
          // Convert data on session length to `AvgSessionLength` case class, for BigQuery storage
          AvgSessionLength(mean, fmt.print(w.start()))
      }
      // Save to the BigQuery table defined by "output" + "_sessions" suffix
      .saveAsTypedBigQuery(args("output") + "_sessions")

    // Close context and execute the pipeline
    val result = sc.close()
    // Wait to finish processing before exiting when streaming pipeline is canceled during shutdown
    exampleUtils.waitToFinish(result.pipelineResult)
  }
  // scalastyle:on method.length

  def calculateSpammyUsers(userScores: SCollection[(String, Int)]): SCollection[(String, Int)] = {
    // Sum of scores by user
    val sumScores = userScores.sumByKey
    // Average of all user scores
    val globalMeanScore = sumScores.values.mean
    sumScores
    // Cross product of global mean and user scores,
    // effectively appending global mean to each (user, score) tuple.
      .cross(globalMeanScore)
      .filter {
        case ((_, score), gmc) =>
          // Filter keeps users who have a score higher than 2.5x the average score
          score > (gmc * 2.5)
      }
      // Keys are the (user, sumScore) tuples
      .keys
  }

}
