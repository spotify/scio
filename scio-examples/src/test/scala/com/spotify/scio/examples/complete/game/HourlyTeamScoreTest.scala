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

import com.spotify.scio.bigquery._
import com.spotify.scio.examples.complete.game.HourlyTeamScore.TeamScoreSums
import com.spotify.scio.io._
import com.spotify.scio.testing._

class HourlyTeamScoreTest extends PipelineSpec {

  val inData = Seq(
    "user0_MagentaKangaroo,MagentaKangaroo,3,1447955630000,2015-11-19 09:53:53.444",
    "user13_ApricotQuokka,ApricotQuokka,15,1447955630000,2015-11-19 09:53:53.444",
    "user6_AmberNumbat,AmberNumbat,11,1447955630000,2015-11-19 09:53:53.444",
    "user7_AlmondWallaby,AlmondWallaby,15,1447955630000,2015-11-19 09:53:53.444",
    "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,12,1447955630000,2015-11-19 09:53:53.444",
    "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,11,1447955630000,2015-11-19 09:53:53.444",
    "user19_BisqueBilby,BisqueBilby,6,1447955630000,2015-11-19 09:53:53.444",
    "user19_BisqueBilby,BisqueBilby,8,1447955630000,2015-11-19 09:53:53.444",
    // time gap...
    "user0_AndroidGreenEchidna,AndroidGreenEchidna,0,1447965690000,2015-11-19 12:41:31.053",
    "user0_MagentaKangaroo,MagentaKangaroo,4,1447965690000,2015-11-19 12:41:31.053",
    "user2_AmberCockatoo,AmberCockatoo,13,1447965690000,2015-11-19 12:41:31.053",
    "user18_BananaEmu,BananaEmu,7,1447965690000,2015-11-19 12:41:31.053",
    "user3_BananaEmu,BananaEmu,17,1447965690000,2015-11-19 12:41:31.053",
    "user18_BananaEmu,BananaEmu,1,1447965690000,2015-11-19 12:41:31.053",
    "user18_ApricotCaneToad,ApricotCaneToad,14,1447965690000,2015-11-19 12:41:31.053")

  val expected = Seq(
    TeamScoreSums("AlmondWallaby", 15, "2015-11-19 09:00:00.000"),
    TeamScoreSums("AmberCockatoo", 13, "2015-11-19 12:00:00.000"),
    TeamScoreSums("AmberNumbat", 11, "2015-11-19 09:00:00.000"),
    TeamScoreSums("AndroidGreenEchidna", 0, "2015-11-19 12:00:00.000"),
    TeamScoreSums("AndroidGreenKookaburra", 23, "2015-11-19 09:00:00.000"),
    TeamScoreSums("ApricotCaneToad", 14, "2015-11-19 12:00:00.000"),
    TeamScoreSums("ApricotQuokka", 15, "2015-11-19 09:00:00.000"),
    TeamScoreSums("BananaEmu", 25, "2015-11-19 12:00:00.000"),
    TeamScoreSums("BisqueBilby", 14, "2015-11-19 09:00:00.000"),
    TeamScoreSums("MagentaKangaroo", 3, "2015-11-19 09:00:00.000"),
    TeamScoreSums("MagentaKangaroo", 4, "2015-11-19 12:00:00.000"))

  "HourlyTeamScore" should "work" in {
    JobTest[com.spotify.scio.examples.complete.game.HourlyTeamScore.type]
      .args("--input=in.txt", "--output=dataset.table")
      .input(TextIO("in.txt"), inData)
      .output(BigQueryIO[TeamScoreSums]("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

}
