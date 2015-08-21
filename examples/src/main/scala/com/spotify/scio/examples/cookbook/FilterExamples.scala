package com.spotify.scio.examples.cookbook

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.bigquery._
import com.spotify.scio._

import scala.collection.JavaConverters._

case class Record(year: Int, month: Int, day: Int, meanTemp: Double)

/*
SBT
runMain
  com.spotify.scio.examples.FilterExamples
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --output=[DATASET].filter_examples
*/

object FilterExamples {
  val WEATHER_SAMPLES_TABLE = "clouddataflow-readonly:samples.weather_stations"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("year").setType("INTEGER"),
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("day").setType("INTEGER"),
      new TableFieldSchema().setName("mean_temp").setType("FLOAT")
    ).asJava)

    val monthFilter = args.getOrElse("monthFilter", "7").toInt

    val pipe = sc.bigQueryTable(args.getOrElse("input", WEATHER_SAMPLES_TABLE))
      .map { row =>
        val year = row.getInt("year")
        val month = row.getInt("month")
        val day = row.getInt("day")
        val meanTemp = row.getDouble("mean_temp")
        Record(year, month, day, meanTemp)
      }

    val globalMeanTemp = pipe.map(_.meanTemp).mean()

    pipe
      .filter(_.month == monthFilter)
      .cross(globalMeanTemp)
      .filter(kv => kv._1.meanTemp < kv._2)
      .keys
      .map(r => TableRow("year" -> r.year, "month" -> r.month, "day" -> r.day, "mean_temp" -> r.meanTemp))
      .saveAsBigQuery(args("output"), schema, CREATE_IF_NEEDED, WRITE_TRUNCATE)

    sc.close()
  }

}
