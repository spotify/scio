package com.spotify.cloud.dataflow.examples

import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow._

/*
SBT
runMain
  com.spotify.cloud.dataflow.examples.JoinExamples
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --output=gs://[BUCKET]/dataflow/join_examples
*/

object JoinUtil {

  val EVENT_TABLE = "gdelt-bq:gdeltv2.events"
  val COUNTRY_TABLE = "gdelt-bq:full.crosswalk_geocountrycodetohuman"

  def extractEventInfo(row: TableRow): Seq[(String, String)] = {
    val countryCode = row.get("ActionGeo_CountryCode").asInstanceOf[String]
    val sqlDate = row.get("SQLDATE").asInstanceOf[String]
    val actor1Name = row.get("Actor1Name").asInstanceOf[String]
    val sourceUrl = row.get("SOURCEURL").asInstanceOf[String]
    val eventInfo = s"Date: $sqlDate, Actor1: $actor1Name, url: $sourceUrl"

    if (countryCode == null || eventInfo == null) Seq() else Seq((countryCode, eventInfo))
  }

  def extractCountryInfo(row: TableRow): (String, String) = {
    val countryCode = row.get("FIPSCC").asInstanceOf[String]
    val countryName = row.get("HumanName").asInstanceOf[String]
    (countryCode, countryName)
  }

  def formatOutput(countryCode: String, countryName: String, eventInfo: String): String =
    s"Country code: $countryCode, Country name: $countryName, Event info: $eventInfo"

}

object JoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    val eventsInfo = context.bigQueryTable(EVENT_TABLE).flatMap(extractEventInfo)
    val countryInfo = context.bigQueryTable(COUNTRY_TABLE).map(extractCountryInfo)

    eventsInfo
      .leftOuterJoin(countryInfo)
      .map { t =>
        val (countryCode, (eventInfo, countryNameOpt)) = t
        val countryName = countryNameOpt.getOrElse("none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      .saveAsTextFile(args("output"))

    context.close()
  }
}

object SideInputJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    val eventsInfo = context.bigQueryTable(EVENT_TABLE).flatMap(extractEventInfo)
    val countryInfo = context.bigQueryTable(COUNTRY_TABLE).map(extractCountryInfo)

    eventsInfo
      .withKVSideInput(countryInfo)
      .map { (kv, m) =>
        val (countryCode, eventInfo) = kv
        val countryName = m.getOrElse(countryCode, Seq()).headOption.getOrElse("none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      .toSCollection
      .saveAsTextFile(args("output"))

    context.close()
  }
}

object HashJoinExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    import JoinUtil._

    val eventsInfo = context.bigQueryTable(EVENT_TABLE).flatMap(extractEventInfo)
    val countryInfo = context.bigQueryTable(COUNTRY_TABLE).map(extractCountryInfo)

    eventsInfo
      .hashLeftJoin(countryInfo)
      .map { t =>
        val (countryCode, (eventInfo, countryNameOpt)) = t
        val countryName = countryNameOpt.getOrElse("none")
        formatOutput(countryCode, countryName, eventInfo)
      }
      .saveAsTextFile(args("output"))

    context.close()
  }
}
