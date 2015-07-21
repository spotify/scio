// INCOMPLETE
package com.spotify.cloud.dataflow.examples

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.cloud.bigquery._
import com.spotify.cloud.dataflow._
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.cloud.dataflow.examples.WindowedWordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=[DATASET].windowed_wordcount
*/

object WindowedWordCount {

  val RAND_RANGE = 7200000
  val WINDOW_SIZE = 1

  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING"),
      new TableFieldSchema().setName("count").setType("INTEGER"),
      new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP")
    ).asJava)

    context
      .textFile(args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt"))
      .toWindowed
      .map { wv =>
        val randomTimestamp = System.currentTimeMillis() - (scala.math.random * RAND_RANGE).toLong
        wv.copy(value = wv.value, timestamp = new Instant(randomTimestamp))
      }
      .toSCollection
      .withFixedWindows(Duration.standardMinutes(args.optional("windowSize").map(_.toInt).getOrElse(WINDOW_SIZE)))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .toWindowed
      .map { wv =>
        wv.copy(value = TableRow("word" -> wv.value._1, "count" -> wv.value._2, "window_timestamp" -> wv.timestamp))
      }
      .toSCollection
      .saveAsBigQuery(args("output"), schema, CREATE_IF_NEEDED, WRITE_TRUNCATE)

    context.close()
  }

}