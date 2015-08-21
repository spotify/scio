package com.spotify.scio.examples.extra

import com.spotify.scio.bigquery._
import com.spotify.scio._
import org.joda.time.Instant

/*
SBT
runMain
  com.spotify.scio.examples.DistCacheExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/wikipedia_edits/wiki_data-*.json
  --output=gs://[BUCKET]/dataflow/dist_cache_example
*/

object DistCacheExample {

  val EXPORTED_WIKI_TABLE = "gs://dataflow-samples/wikipedia_edits/*.json"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    val dc = context.distCache("gs://dataflow-samples/samples/misc/months.txt") { f =>
      scala.io.Source.fromFile(f).getLines().map { s =>
        val t = s.split(" ")
        (t(0).toInt, t(1))
      }.toMap
    }

    context
      .tableRowJsonFile(args.getOrElse("input", EXPORTED_WIKI_TABLE))
      .map(row => new Instant(row.getLong("timestamp") * 1000L).toDateTime.getMonthOfYear)
      .countByValue()
      .map(kv => dc().getOrElse(kv._1, "unknown") + " " + kv._2)
      .saveAsTextFile(args("output"))

    context.close()
  }

}
