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

// Example: Calculate the score for a user

// Usage:

// `sbt runMain "com.spotify.scio.examples.complete.game.UserScore
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --output=bq://[PROJECT]/[DATASET]/mobile_game_user_score"`

package com.spotify.scio.examples.complete.game

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData

import scala.util.Try

object UserScore {

  // Case class containing all the fields within an event, for internal model
  case class GameActionInfo(user: String, team: String, score: Int, timestamp: Long)

  // The schema for the BigQuery table to write output to is defined as an annotated case class
  @BigQueryType.toTable
  case class UserScoreSums(user: String, total_score: Int)

  // Helper function for parsing data. Reads in a CSV line and converts to `GameActionInfo` instance
  def parseEvent(line: String): Option[GameActionInfo] =
    Try {
      val t = line.split(",")
      GameActionInfo(t(0).trim, t(1).trim, t(2).toInt, t(3).toLong)
    }.toOption

  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    // A text file containing data on events
    val input = args.getOrElse("input", ExampleData.GAMING)

    sc.textFile(input)
      // Parse each line as `GameActionInfo` events, keep the ones that successfully parsed
      .flatMap(parseEvent)
      // Change each event into a tuple of: user, and that user's score
      .map(i => (i.user, i.score))
      // Sum the scores by user
      .sumByKey
      // Map summed results from tuples into `UserScoreSums` case class, so we can save to BQ
      .map(UserScoreSums.tupled)
      // Save to the BigQuery table defined by "output" in the arguments passed in
      .saveAsTypedBigQuery(args("output"))

    // Close context and execute the pipeline
    sc.close()
  }

}
