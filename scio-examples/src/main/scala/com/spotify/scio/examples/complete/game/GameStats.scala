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

  @BigQueryType.toTable
  case class TeamScoreSums(team: String, total_score: Int,
                           window_start: String, processing_time: String)

  @BigQueryType.toTable
  case class AvgSessionLength(mean_duration: Double, window_start: String)

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    sc.optionsAs[StreamingOptions].setStreaming(true)
    val exampleUtils = new ExampleUtils(sc.options)

    def fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")))
    val fixedWindowDuration = args.int("fixedWindowDuration", 60)
    val sessionGap = args.int("sessionGap", 5)
    val userActivityWindowDuration = args.int("userActivityWindowDuration", 30)

    val rawEvents = sc.pubsubTopic(args("topic"), idAttribute = "timestamp_ms")
      .flatMap(UserScore.parseEvent)

    val userEvents = rawEvents.map(i => (i.user, i.score))
    val userScores = userEvents.withFixedWindows(Duration.standardMinutes(fixedWindowDuration))
    val spammyUsers = calculateSpammyUsers(userScores).asMapSideInput

    rawEvents
      .withFixedWindows(Duration.standardMinutes(fixedWindowDuration))
      .withSideInputs(spammyUsers)
      .filter { case (i, s) => s(spammyUsers).contains(i.user) }
      .toSCollection
      .map(i => (i.team, i.score))
      .sumByKey
      .toWindowed
      .map { wv =>
        val start = fmt.print(wv.window.asInstanceOf[IntervalWindow].start())
        val now = fmt.print(Instant.now())
        wv.copy(value = TeamScoreSums(wv.value._1, wv.value._2, start, now))
      }
      .toSCollection
      .saveAsTypedBigQuery(args("output") + "_team")

    userEvents
      .withSessionWindows(
        Duration.standardMinutes(sessionGap),
        options = WindowOptions(timestampCombiner = TimestampCombiner.END_OF_WINDOW))
      .keys.distinct
      .withWindow[IntervalWindow]
      .map { case (_, w) =>
        new Duration(w.start(), w.end()).toPeriod().toStandardMinutes.getMinutes
      }
      .withFixedWindows(Duration.standardMinutes(userActivityWindowDuration))
      .mean
      .withWindow[IntervalWindow]
      .map { case (mean, w) =>
        AvgSessionLength(mean, fmt.print(w.start()))
      }
      .saveAsTypedBigQuery(args("output") + "_sessions")

    val result = sc.close()
    exampleUtils.waitToFinish(result.internal)
  }
  // scalastyle:on method.length

  def calculateSpammyUsers(userScores: SCollection[(String, Int)]): SCollection[(String, Int)] = {
    val sumScores = userScores.sumByKey
    val globalMeanScore = sumScores.values.mean
    sumScores
      .cross(globalMeanScore)
      .filter { case ((_, score), gmc) =>
        score > (gmc * 2.5)
      }
      .keys
  }

}
