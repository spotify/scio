package com.spotify.scio.examples.complete

import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow
import com.spotify.scio._
import com.spotify.scio.bigquery._
import org.joda.time.{Duration, Instant}

/*
SBT
runMain
  com.spotify.scio.examples.complete.TopWikipediaSessions
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/wikipedia_edits/wiki_data-*.json
  --output=gs://[BUCKET]/dataflow/top_wikipedia_sessions
*/

object TopWikipediaSessions {

  val EXPORTED_WIKI_TABLE = "gs://dataflow-samples/wikipedia_edits/*.json"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val samplingThreshold = 0.1

    sc
      .tableRowJsonFile(args.getOrElse("input", EXPORTED_WIKI_TABLE))
      .flatMap { row =>
        try Seq((row.getString("contributor_username"), row.getInt("timestamp"))) catch {
          case e: NullPointerException => None
        }
      }
      .timestampBy(kv => new Instant(kv._2 * 1000L))
      .map(_._1)
      .sample(withReplacement = false, fraction = samplingThreshold)
      .withSessionWindows(Duration.standardHours(1))
      .countByValue()
      .toWindowed
      .map(wv => wv.copy((wv.value._1 + " : " + wv.window, wv.value._2)))
      .toSCollection
      .windowByMonths(1)
      .top(1)(Ordering.by(_._2))
      .toWindowed
      .flatMap { wv =>
        wv.value.map { kv =>
          val o = kv._1 + " : " + kv._2 + " : " + wv.window.asInstanceOf[IntervalWindow].start()
          wv.copy(value = o)
        }
      }
      .toSCollection
      .saveAsTextFile(args("output"))

    sc.close()
  }

}
