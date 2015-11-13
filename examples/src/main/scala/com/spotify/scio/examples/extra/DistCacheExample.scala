package com.spotify.scio.examples.extra

import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import org.joda.time.Instant

/*
SBT
runMain
  com.spotify.scio.examples.extra.DistCacheExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/wikipedia_edits/wiki_data-*.json
  --output=gs://[BUCKET]/[PATH]/dist_cache_example
*/

object DistCacheExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val dc = sc.distCache(ExampleData.MONTHS) { f =>
      scala.io.Source.fromFile(f).getLines().map { s =>
        val t = s.split(" ")
        (t(0).toInt, t(1))
      }.toMap
    }

    sc
      .tableRowJsonFile(args.getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
      .map(row => new Instant(row.getLong("timestamp") * 1000L).toDateTime.getMonthOfYear)
      .countByValue()
      .map(kv => dc().getOrElse(kv._1, "unknown") + " " + kv._2)
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
