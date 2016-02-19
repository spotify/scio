/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio.examples.complete

import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import org.joda.time.{Duration, Instant}

/*
SBT
runMain
  com.spotify.scio.examples.complete.TopWikipediaSessions
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/wikipedia_edits/wiki_data-*.json
  --output=gs://[BUCKET]/[PATH]/top_wikipedia_sessions
*/

object TopWikipediaSessions {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val samplingThreshold = 0.1

    sc
      .tableRowJsonFile(args.getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
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
