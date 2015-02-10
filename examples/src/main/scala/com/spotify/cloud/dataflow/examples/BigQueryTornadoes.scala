package com.spotify.cloud.dataflow.examples

import com.google.api.services.bigquery.model.{TableSchema, TableFieldSchema}
import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow._

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.cloud.dataflow.examples.BigQueryTornadoes
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --output=[DATASET].bigquery_tornadoes
*/

object BigQueryTornadoes {

  val WEATHER_SAMPLES_TABLE = "publicdata:samples.gsod"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    val fields = List(
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("tornado_count").setType("INTEGER"))
    val schema = new TableSchema().setFields(fields.asJava)

    context
      .bigQueryTable(args.getOrElse("input", WEATHER_SAMPLES_TABLE))
      .flatMap(row => if (row.getBoolean("tornado")) Seq(row.getInt("month")) else Seq())
      .countByValue()
      .map(kv => TableRow("month" -> kv._1, "tornado_count" -> kv._2))
      .saveAsBigQuery(args("output"), schema, CREATE_IF_NEEDED, WRITE_TRUNCATE)

    context.close()
  }

}
