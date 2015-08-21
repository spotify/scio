package com.spotify.scio.examples.cookbook

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.bigquery._
import com.spotify.scio._

import scala.collection.JavaConverters._
import scala.collection.SortedSet

/*
SBT
runMain
  com.spotify.scio.examples.cookbook.CombinePerKeyExamples
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --output=[DATASET].combine_per_key_examples
*/

object CombinePerKeyExamples {

  val SHAKESPEARE_TABLE = "publicdata:samples.shakespeare"
  val MIN_WORD_LENGTH = 9

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING"),
      new TableFieldSchema().setName("all_plays").setType("STRING")
    ).asJava)

    sc.bigQueryTable(args.getOrElse("input", SHAKESPEARE_TABLE))
      .flatMap { row =>
        val playName = row.getString("corpus")
        val word = row.getString("word")
        if (word.length > MIN_WORD_LENGTH) Seq((word, playName)) else Nil
      }
      // Sort values to make test happy
      .aggregateByKey(SortedSet[String]())(_ + _, _ ++ _)
      .mapValues(_.mkString(","))
      .map(kv => TableRow("word" -> kv._1, "all_plays" -> kv._2))
      .saveAsBigQuery(args("output"), schema, CREATE_IF_NEEDED, WRITE_TRUNCATE)

    sc.close()
  }

}
