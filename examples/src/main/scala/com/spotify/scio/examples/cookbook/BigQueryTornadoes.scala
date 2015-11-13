package com.spotify.scio.examples.cookbook

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.experimental._

import scala.collection.JavaConverters._

/*
runMain
  com.spotify.scio.examples.cookbook.BigQueryTornadoes
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=clouddataflow-readonly:samples.weather_stations
  --output=[DATASET].bigquery_tornadoes
*/

object BigQueryTornadoes {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("tornado_count").setType("INTEGER")
    ).asJava)

    sc
      .bigQueryTable(args.getOrElse("input", ExampleData.WEATHER_SAMPLES_TABLE))
      .flatMap(r => if (r.getBoolean("tornado")) Seq(r.getInt("month")) else Nil)
      .countByValue()
      .map(kv => TableRow("month" -> kv._1, "tornado_count" -> kv._2))
      .saveAsBigQuery(args("output"), schema, CREATE_IF_NEEDED, WRITE_TRUNCATE)

    sc.close()
  }
}
