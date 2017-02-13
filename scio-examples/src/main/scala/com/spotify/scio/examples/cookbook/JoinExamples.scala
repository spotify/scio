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

package com.spotify.scio.examples.cookbook

import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

/*
SBT
runMain
  com.spotify.scio.examples.cookbook.JoinExamples
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --gcpTempLocation=gs://[BUCKET]/path/to/staging
  --output=gs://[BUCKET]/[PATH]/join_examples
*/

object JoinUtil {

  def extractEventInfo(row: TableRow): Seq[(String, String)] = {
    val countryCode = row.getString("ActionGeo_CountryCode")
    val sqlDate = row.getString("SQLDATE")
    val actor1Name = row.getString("Actor1Name")
    val sourceUrl = row.getString("SOURCEURL")
    val eventInfo = s"Date: $sqlDate, Actor1: $actor1Name, url: $sourceUrl"

    if (countryCode == null || eventInfo == null) Nil else Seq((countryCode, eventInfo))
  }

  def extractCountryInfo(row: TableRow): (String, String) = {
    val countryCode = row.getString("FIPSCC")
    val countryName = row.getString("HumanName")
    (countryCode, countryName)
  }

  def formatOutput(countryCode: String, countryName: String, eventInfo: String): String =
    s"Country code: $countryCode, Country name: $countryName, Event info: $eventInfo"

}

object JoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    val eventsInfo = sc.bigQueryTable(ExampleData.EVENT_TABLE).flatMap(extractEventInfo)
    val countryInfo = sc.bigQueryTable(ExampleData.COUNTRY_TABLE).map(extractCountryInfo)

    eventsInfo
      .leftOuterJoin(countryInfo)  // regular left outer join
      .map { t =>
      val (countryCode, (eventInfo, countryNameOpt)) = t
      val countryName = countryNameOpt.getOrElse("none")
      formatOutput(countryCode, countryName, eventInfo)
    }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}

object SideInputJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    val eventsInfo = sc.bigQueryTable(ExampleData.EVENT_TABLE).flatMap(extractEventInfo)
    val countryInfo = sc.bigQueryTable(ExampleData.COUNTRY_TABLE).map(extractCountryInfo)
      .asMapSideInput

    eventsInfo
      .withSideInputs(countryInfo)  // replicate right hand side to all workers as side input
      .map { (kv, side) =>
      val (countryCode, eventInfo) = kv
      val m = side(countryInfo)
      val countryName = m.getOrElse(countryCode, "none")
      formatOutput(countryCode, countryName, eventInfo)
    }
      .toSCollection
      .saveAsTextFile(args("output"))

    sc.close()
  }
}

object HashJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    val eventsInfo = sc.bigQueryTable(ExampleData.EVENT_TABLE).flatMap(extractEventInfo)
    val countryInfo = sc.bigQueryTable(ExampleData.COUNTRY_TABLE).map(extractCountryInfo)

    eventsInfo
      .hashLeftJoin(countryInfo)  // shortcut for side input join
      .map { t =>
      val (countryCode, (eventInfo, countryNameOpt)) = t
      val countryName = countryNameOpt.getOrElse("none")
      formatOutput(countryCode, countryName, eventInfo)
    }
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
