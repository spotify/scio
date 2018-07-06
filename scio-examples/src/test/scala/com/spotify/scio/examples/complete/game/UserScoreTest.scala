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
import com.spotify.scio.examples.complete.game.UserScore.UserScoreSums
import com.spotify.scio.testing._

class UserScoreTest extends PipelineSpec {

  val inData1 = Seq(
    "user0_MagentaKangaroo,MagentaKangaroo,3,1447955630000,2015-11-19 09:53:53.444",
    "user13_ApricotQuokka,ApricotQuokka,15,1447955630000,2015-11-19 09:53:53.444",
    "user6_AmberNumbat,AmberNumbat,11,1447955630000,2015-11-19 09:53:53.444",
    "user7_AlmondWallaby,AlmondWallaby,15,1447955630000,2015-11-19 09:53:53.444",
    "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,12,1447955630000,2015-11-19 09:53:53.444",
    "user6_AliceBlueDingo,AliceBlueDingo,4,xxxxxxx,2015-11-19 09:53:53.444",
    "user7_AndroidGreenKookaburra,AndroidGreenKookaburra,11,1447955630000,2015-11-19 09:53:53.444",
    "THIS IS A PARSE ERROR,2015-11-19 09:53:53.444",
    "user19_BisqueBilby,BisqueBilby,6,1447955630000,2015-11-19 09:53:53.444",
    "user19_BisqueBilby,BisqueBilby,8,1447955630000,2015-11-19 09:53:53.444")

  val inData2 = Seq(
    "user6_AliceBlueDingo,AliceBlueDingo,4,xxxxxxx,2015-11-19 09:53:53.444",
    "THIS IS A PARSE ERROR,2015-11-19 09:53:53.444",
    "user13_BisqueBilby,BisqueBilby,xxx,1447955630000,2015-11-19 09:53:53.444")

  val expected = Seq(
    UserScoreSums("user0_MagentaKangaroo", 3),
    UserScoreSums("user13_ApricotQuokka", 15),
    UserScoreSums("user6_AmberNumbat", 11),
    UserScoreSums("user7_AlmondWallaby", 15),
    UserScoreSums("user7_AndroidGreenKookaburra", 23),
    UserScoreSums("user19_BisqueBilby", 14))

  "UserScore" should "work" in {
    JobTest[com.spotify.scio.examples.complete.game.UserScore.type]
      .args("--input=in.txt", "--output=dataset.table")
      .input(TextIO("in.txt"), inData1)
      .output(BigQueryIO[UserScoreSums]("dataset.table"))(_ should containInAnyOrder (expected))
      .run()
  }

  it should "drop bad input" in {
    JobTest[com.spotify.scio.examples.complete.game.UserScore.type]
      .args("--input=in.txt", "--output=dataset.table")
      .input(TextIO("in.txt"), inData2)
      .output(BigQueryIO[UserScoreSums]("dataset.table"))(_ should beEmpty)
      .run()
  }

}
