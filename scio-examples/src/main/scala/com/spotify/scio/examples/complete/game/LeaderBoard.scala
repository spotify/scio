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
import com.spotify.scio.examples.complete.game.UserScore.GameActionInfo
import com.spotify.scio.streaming._
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, Duration, Instant}

object LeaderBoard {

  @BigQueryType.toTable
  case class TeamScoreSums(team: String, total_score: Int,
                           window_start: String, processing_time: String, timing: String)

  @BigQueryType.toTable
  case class UserScoreSums(user: String, total_score: Int, processing_time: String)

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    sc.optionsAs[StreamingOptions].setStreaming(true)
    val exampleUtils = new ExampleUtils(sc.options)

    def fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")))
    val teamWindowDuration = Duration.standardMinutes(args.int("teamWindowDuration", 60))
    val allowedLateness = Duration.standardMinutes(args.int("allowedLateness", 120))

    val gameEvents = sc.pubsubTopic(args("topic"), timestampAttribute = "timestamp_ms")
      .flatMap(UserScore.parseEvent)

    calculateTeamScores(gameEvents, teamWindowDuration, allowedLateness)
      .toWindowed
      .map { wv =>
        val start = fmt.print(wv.window.asInstanceOf[IntervalWindow].start())
        val now = fmt.print(Instant.now())
        val timing = wv.pane.getTiming.toString
        wv.copy(value = TeamScoreSums(wv.value._1, wv.value._2, start, now, timing))
      }
      .toSCollection
      .saveAsTypedBigQuery(args("output") + "_team")

    gameEvents
      .withGlobalWindow(WindowOptions(
        trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
          .plusDelayOf(Duration.standardMinutes(10))),
        accumulationMode = ACCUMULATING_FIRED_PANES,
        allowedLateness = allowedLateness)
      )
      .map(i => (i.user, i.score))
      .sumByKey
      .map(kv => UserScoreSums(kv._1, kv._2, fmt.print(Instant.now())))
      .saveAsTypedBigQuery(args("output") + "_user")

    val result = sc.close()
    exampleUtils.waitToFinish(result.internal)
  }
  // scalastyle:on method.length

  def calculateTeamScores(infos: SCollection[GameActionInfo],
                          teamWindowDuration: Duration,
                          allowedLateness: Duration): SCollection[(String, Int)] =
    infos.withFixedWindows(
      teamWindowDuration,
      options = WindowOptions(
        trigger = AfterWatermark.pastEndOfWindow()
          .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(5)))
          .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(10))),
        accumulationMode = ACCUMULATING_FIRED_PANES,
        allowedLateness = allowedLateness))
      .map(i => (i.team, i.score))
      .sumByKey

}
