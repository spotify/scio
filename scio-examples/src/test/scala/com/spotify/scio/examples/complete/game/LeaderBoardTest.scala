/*
 * Copyright 2017 Spotify AB.
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

import com.spotify.scio.examples.complete.game.UserScore.GameActionInfo
import com.spotify.scio.testing._
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.{Duration, Instant}

class LeaderBoardTest extends PipelineSpec {

  private val allowedLateness = Duration.standardHours(1)
  private val teamWindowDuration = Duration.standardMinutes(20)
  private val baseTime = new Instant(0)

  case class TestUser(user: String, team: String)
  private val redOne = TestUser("scarlet", "red")
  private val redTwo = TestUser("burgundy", "red")
  private val blueOne = TestUser("navy", "blue")
  private val blueTwo = TestUser("sky", "blue")

  private def event(
    user: TestUser,
    score: Int,
    baseTimeOffset: Duration): TimestampedValue[GameActionInfo] = {
    val t = baseTime.plus(baseTimeOffset)
    TimestampedValue.of(
      GameActionInfo(user.user, user.team, score, t.getMillis),
      t)
  }

  "LeaderBoard.calculateTeamScores" should "work with on time elements" in {
    val stream = testStreamOf[GameActionInfo]
    // Start at the epoch
      .advanceWatermarkTo(baseTime)
      // add some elements ahead of the watermark
      .addElements(
        event(blueOne, 3, Duration.standardSeconds(3)),
        event(blueOne, 2, Duration.standardMinutes(1)),
        event(redTwo, 3, Duration.standardSeconds(22)),
        event(blueTwo, 5, Duration.standardSeconds(3))
      )
      // The watermark advances slightly, but not past the end of the window
      .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
      .addElements(event(redOne, 1, Duration.standardMinutes(4)),
                   event(blueOne, 2, Duration.standardSeconds(270)))
      // The window should close and emit an ON_TIME pane
      .advanceWatermarkToInfinity

    runWithContext { sc =>
      val teamScores = LeaderBoard.calculateTeamScores(sc.testStream(stream),
                                                       teamWindowDuration,
                                                       allowedLateness)

      val window = new IntervalWindow(baseTime, teamWindowDuration)
      teamScores should inOnTimePane(window) {
        containInAnyOrder(Seq((blueOne.team, 12), (redOne.team, 4)))
      }
    }
  }

}
