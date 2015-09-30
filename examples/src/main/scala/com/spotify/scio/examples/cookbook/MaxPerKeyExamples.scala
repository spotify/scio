package com.spotify.scio.examples.cookbook

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.bigquery._
import com.spotify.scio._

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.BigQueryTornadoes
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --output=[DATASET].max_per_key_examples
*/

object MaxPerKeyExamples {

  val WEATHER_SAMPLE_TABLE = "clouddataflow-readonly:samples.weather_stations"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("max_mean_temp").setType("FLOAT")).asJava)

    sc
      .bigQueryTable(args.getOrElse("input", WEATHER_SAMPLE_TABLE))
      .map(row => (row.getInt("month"), row.getDouble("mean_temp")))
      .maxByKey
      .map(kv => TableRow("month" -> kv._1, "max_mean_temp" -> kv._2))
      .saveAsBigQuery(args("output"), schema, CREATE_IF_NEEDED, WRITE_TRUNCATE)

    sc.close()
  }

}
