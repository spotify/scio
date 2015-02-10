package com.spotify.cloud.dataflow.examples

import com.spotify.cloud.dataflow._
import com.spotify.cloud.dataflow.values.WindowedValue
import org.joda.time.{Duration, Instant}

/*
SBT
runMain
  com.spotify.cloud.dataflow.examples.WindowingWordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/dataflow/windowing_wordcount
*/

object WindowingWordCount {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (context, args) = ContextAndArgs(cmdlineArgs)

    context
      .textFile(args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt"))
      .toWindowed
      .flatMap { wv =>
        wv.value.split("[^a-zA-Z']+").filter(_.nonEmpty).map { s =>
          wv.copy(value = s, timestamp = new Instant(System.currentTimeMillis()))
        }
      }
      .toSCollection
      .withFixedWindows(Duration.millis(1))
      .countByValue()
      .toWindowed
      .map { wv =>
        val (k, v) = wv.value
        wv.copy(s"Element: $k Value: $v Timestamp: ${wv.timestamp}} Windows: (${wv.timestamp}})")
      }
      .toSCollection
      .saveAsTextFile(args("output"))

    context.close()
  }

}
