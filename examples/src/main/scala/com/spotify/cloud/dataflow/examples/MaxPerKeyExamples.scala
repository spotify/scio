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
  --output=[DATASET].max_per_key_examples
*/

object MaxPerKeyExamples {

  val WEATHER_SAMPLE_TABLE = "publicdata:samples.gsod"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    val fields = List(
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("max_mean_temp").setType("FLOAT"))
    val schema = new TableSchema().setFields(fields.asJava)

    context
      .bigQueryTable(args.getOrElse("input", WEATHER_SAMPLE_TABLE))
      .map(row => (row.getInt("month"), row.getDouble("mean_temp")))
      .maxByKey()
      .map(kv => TableRow("month" -> kv._1, "max_mean_temp" -> kv._2))
      .saveAsBigQuery(args("output"), schema, CREATE_IF_NEEDED, WRITE_TRUNCATE)

    context.close()
  }

}
