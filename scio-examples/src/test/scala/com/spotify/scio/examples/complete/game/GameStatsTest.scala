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

import com.spotify.scio.testing._

class GameStatsTest extends PipelineSpec {

  "GameStats.calculateSpammyUsers" should "work" in {
    val userScores = Seq(
      ("Robot-2", 66),
      ("Robot-1", 116),
      ("user7_AndroidGreenKookaburra", 23),
      ("user7_AndroidGreenKookaburra", 1),
      ("user19_BisqueBilby", 14),
      ("user13_ApricotQuokka", 15),
      ("user18_BananaEmu", 25),
      ("user6_AmberEchidna", 8),
      ("user2_AmberQuokka", 6),
      ("user0_MagentaKangaroo", 4),
      ("user0_MagentaKangaroo", 3),
      ("user2_AmberCockatoo", 13),
      ("user7_AlmondWallaby", 15),
      ("user6_AmberNumbat", 11),
      ("user6_AmberQuokka", 4)
    )
    val spammers = Seq(("Robot-2", 66), ("Robot-1", 116))
    runWithContext { sc =>
      val p = GameStats.calculateSpammyUsers(sc.parallelize(userScores))
      p should containInAnyOrder(spammers)
    }
  }

}
