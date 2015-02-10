package com.spotify.cloud.dataflow.examples

import com.google.api.services.bigquery.model.{TableSchema, TableFieldSchema}
import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow._

import scala.collection.JavaConverters._
import scala.collection.SortedSet

/*
SBT
runMain
  com.spotify.cloud.dataflow.examples.CombinePerKeyExamples
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --output=[DATASET].combine_per_key_examples
*/

object CombinePerKeyExamples {

  val SHAKESPEARE_TABLE = "publicdata:samples.shakespeare"
  val MIN_WORD_LENGTH = 9

  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    val fields = List(
      new TableFieldSchema().setName("word").setType("STRING"),
      new TableFieldSchema().setName("all_plays").setType("STRING"))
    val schema = new TableSchema().setFields(fields.asJava)

    context.bigQueryTable(args.getOrElse("input", SHAKESPEARE_TABLE))
      .flatMap { row =>
        val playName = row.get("corpus").toString
        val word = row.get("word").toString
        if (word.length > MIN_WORD_LENGTH) Seq((word, playName)) else Seq()
      }
      // sort values to make test happy
      .aggregateByKey(SortedSet[String]())(_ + _, _ ++ _)
      .mapValues(_.mkString(","))
      .map(kv => TableRow("word" -> kv._1, "all_plays" -> kv._2))
      .saveAsBigQuery(args("output"), schema, CREATE_IF_NEEDED, WRITE_TRUNCATE)

    context.close()
  }

}

