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

// Example: Join Examples
// Usage:

// `sbt runMain "com.spotify.scio.examples.cookbook.JoinExamples
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --output=gs://[BUCKET]/[PATH]/join_examples"`
package com.spotify.scio.examples.cookbook

import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

// ## Utilities used in all examples
object JoinUtil {

  // Function to extract event information from BigQuery `TableRow`s
  def extractEventInfo(row: TableRow): Seq[(String, String)] = {
    val countryCode = row.getString("ActionGeo_CountryCode")
    val sqlDate = row.getString("SQLDATE")
    val actor1Name = row.getString("Actor1Name")
    val sourceUrl = row.getString("SOURCEURL")
    val eventInfo = s"Date: $sqlDate, Actor1: $actor1Name, url: $sourceUrl"

    if (countryCode == null || eventInfo == null) Nil else Seq((countryCode, eventInfo))
  }

  // Function to extract country information from BigQuery `TableRow`s
  def extractCountryInfo(row: TableRow): (String, String) = {
    val countryCode = row.getString("FIPSCC")
    val countryName = row.getString("HumanName")
    (countryCode, countryName)
  }

  // Function to format output string
  def formatOutput(countryCode: String, countryName: String, eventInfo: String): String =
    s"Country code: $countryCode, Country name: $countryName, Event info: $eventInfo"

}

// ## Regular shuffle-based join
object JoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    // Extract both sides as `SCollection[(String, String)]`s
    val eventsInfo = sc.bigQueryTable(ExampleData.EVENT_TABLE).flatMap(extractEventInfo)
    val countryInfo = sc.bigQueryTable(ExampleData.COUNTRY_TABLE).map(extractCountryInfo)

    eventsInfo
      // Left outer join to produce `SCollection[(String, (String, Option[String]))]
      .leftOuterJoin(countryInfo)
      .map { t =>
        val (countryCode, (eventInfo, countryNameOpt)) = t
        val countryName = countryNameOpt.getOrElse("none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}

// ## Join with map side input
object SideInputJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    // Extract both sides as `SCollection[(String, String)]`s, and then convert right hand side as
    // a `SideInput` of `Map[String, String]`
    val eventsInfo = sc.bigQueryTable(ExampleData.EVENT_TABLE).flatMap(extractEventInfo)
    val countryInfo = sc.bigQueryTable(ExampleData.COUNTRY_TABLE).map(extractCountryInfo)
      .asMapSideInput

    eventsInfo
      // Replicate right hand side to all workers as a side input
      .withSideInputs(countryInfo)
      // Specialized version of `map` with access to side inputs via `SideInputContext`
      .map { (kv, side) =>
        val (countryCode, eventInfo) = kv
        // Retrieve side input value (`Map[String, String]`)
        val m = side(countryInfo)
        val countryName = m.getOrElse(countryCode, "none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      // End of side input operation, convert back to regular `SCollection`
      .toSCollection
      .saveAsTextFile(args("output"))

    sc.close()
  }
}

// ## Hash join
object HashJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    // Extract both sides as `SCollection[(String, String)]`s
    val eventsInfo = sc.bigQueryTable(ExampleData.EVENT_TABLE).flatMap(extractEventInfo)
    val countryInfo = sc.bigQueryTable(ExampleData.COUNTRY_TABLE).map(extractCountryInfo)

    eventsInfo
      // Hash join uses side input under the hood and is a drop-in replacement for regular join
      .hashLeftJoin(countryInfo)
      .map { t =>
        val (countryCode, (eventInfo, countryNameOpt)) = t
        val countryName = countryNameOpt.getOrElse("none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
