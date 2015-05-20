package com.spotify.cloud.dataflow.examples

import com.google.api.services.bigquery.model.{TableSchema, TableFieldSchema}
import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow._

import scala.collection.JavaConverters._

case class Record(year: Int, month: Int, day: Int, meanTemp: Double)

/*
SBT
runMain
  com.spotify.cloud.dataflow.examples.FilterExamples
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --output=[DATASET].filter_examples
*/

object FilterExamples {
  val WEATHER_SAMPLES_TABLE = "publicdata:samples.gsod"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    val fields = List(
      new TableFieldSchema().setName("year").setType("INTEGER"),
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("day").setType("INTEGER"),
      new TableFieldSchema().setName("mean_temp").setType("FLOAT"))
    val schema = new TableSchema().setFields(fields.asJava)

    def recordToRow(r: Record): TableRow =
      TableRow("year" -> r.year, "month" -> r.month, "day" -> r.day, "mean_temp" -> r.meanTemp)

    val monthFilter = args.getOrElse("monthFilter", "7").toInt

    val pipe = context.bigQueryTable(args.getOrElse("input", WEATHER_SAMPLES_TABLE))
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
      .map(recordToRow)
      .saveAsBigQuery(args("output"), schema, CREATE_IF_NEEDED, WRITE_TRUNCATE)

    context.close()
  }

}
