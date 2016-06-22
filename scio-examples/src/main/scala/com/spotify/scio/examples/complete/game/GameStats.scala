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

import com.google.cloud.dataflow.examples.common.DataflowExampleUtils
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.transforms.windowing.{IntervalWindow, OutputTimeFns}
import com.spotify.scio._
import com.spotify.scio.experimental._
import com.spotify.scio.values.WindowOptions
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, Duration, Instant}

/**
 * Note - this pipeline requires PubSub topic to be present.
 * Testing PubSub topic doc: [[com.google.cloud.dataflow.examples.complete.game.injector.Injector]]
 *
 * {{{
 * runMain com.spotify.scio.examples.complete.game.GameStats
 *   --topic=projects/<project-id>/topics/<topic-id>
 *   --output=<table-spec>
 *   --project=<project-id>
 *   --zone=<zone-id>
 *   --stagingLocation=<bucket-url>
 *   --runner=BlockingDataflowPipelineRunner
 *   --streaming
 * }}}
 */
object GameStats {

  @BigQueryType.toTable
  case class TeamScoreSums(team: String, total_score: Int,
                           window_start: String, processing_time: String)

  @BigQueryType.toTable
  case class AvgSessionLength(mean_duration: Double, window_start: String)

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val dataflowUtils = new DataflowExampleUtils(sc.optionsAs[DataflowPipelineOptions])

    def fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")))

    val fixedWindowDuration = args.int("fixedWindowDuration", 60)
    val sessionGap = args.int("sessionGap", 5)
    val userActivityWindowDuration = args.int("userActivityWindowDuration", 30)

    val rawEvents = sc.pubsubTopic(args("topic"), idLabel = "timestamp_ms")
      .flatMap(UserScore.parseEvent)

    val userEvents = rawEvents.map(i => (i.user, i.score))
    val sumScores = userEvents
      .withFixedWindows(Duration.standardMinutes(fixedWindowDuration))
      .sumByKey
    val globalMeanScore = sumScores.values.mean
    val spammyUsers = sumScores
      .cross(globalMeanScore)
      .filter { case ((_, score), gmc) =>
        score > (gmc * 2.5)
      }
      .keys.asMapSideInput

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
        options = WindowOptions(outputTimeFn = OutputTimeFns.outputAtEndOfWindow()))
      .keys.distinct
      .withWindow
      .map { kv =>
        val w = kv._2.asInstanceOf[IntervalWindow]
        new Duration(w.start(), w.end()).toPeriod().toStandardMinutes.getMinutes
      }
      .withFixedWindows(Duration.standardMinutes(userActivityWindowDuration))
      .mean
      .withWindow
      .map { case (mean, w) =>
        AvgSessionLength(mean, fmt.print(w.asInstanceOf[IntervalWindow].start()))
      }
      .saveAsTypedBigQuery(args("output") + "_sessions")

    val result = sc.close()
    dataflowUtils.waitToFinish(result.internal)
  }
  // scalastyle:on method.length

}
