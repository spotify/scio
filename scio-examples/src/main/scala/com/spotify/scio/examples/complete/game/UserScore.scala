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

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData

import scala.util.Try

object UserScore {

  case class GameActionInfo(user: String, team: String, score: Int, timestamp: Long)

  @BigQueryType.toTable
  case class UserScoreSums(user: String, total_score: Int)

  def parseEvent(line: String): Option[GameActionInfo] = Try {
    val t = line.split(",")
    GameActionInfo(t(0).trim, t(1).trim, t(2).toInt, t(3).toLong)
  }.toOption

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val input = args.getOrElse("input", ExampleData.GAMING)
    sc.textFile(input)
      .flatMap(parseEvent)
      .map(i => (i.user, i.score))
      .sumByKey
      .map(UserScoreSums.tupled)
      .saveAsTypedBigQuery(args("output"))
    sc.close()
  }

}
