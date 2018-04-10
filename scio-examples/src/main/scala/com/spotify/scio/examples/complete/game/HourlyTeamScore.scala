/*
 * Copyright 2018 Spotify AB.
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
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.{DateTimeZone, Duration, Instant}
import org.joda.time.format.DateTimeFormat

object HourlyTeamScore {

  @BigQueryType.toTable
  case class TeamScoreSums(user: String, total_score: Int, window_start: String)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    def fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")))
    val minFmt = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")))
    val startMin =
      new Instant(minFmt.parseMillis(args.getOrElse("startMin", "1970-01-01-00-00"))).getMillis
    val stopMin =
      new Instant(minFmt.parseMillis(args.getOrElse("stopMin", "2100-01-01-00-00"))).getMillis
    val windowDuration = args.long("windowDuration", 60L)

    val input = args.getOrElse("input", ExampleData.GAMING)
    sc.textFile(input)
      .flatMap(UserScore.parseEvent)
      .filter(i => i.timestamp > startMin && i.timestamp < stopMin)
      .timestampBy(i => new Instant(i.timestamp))
      .withFixedWindows(Duration.standardMinutes(windowDuration))
      .map(i => (i.team, i.score))
      .sumByKey
      .withWindow[IntervalWindow]
      .map { case ((team, score), window) =>
        val start = fmt.print(window.start())
        TeamScoreSums(team, score, start)
      }
      .saveAsTypedBigQuery(args("output"))

    sc.close()
  }

}
