/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

// Use distributed cache inside a job
object DistCacheExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // declare a file to be distributed to all workers and logic to load the file
    val dc = sc.distCache(args.getOrElse("months", ExampleData.MONTHS)) { f =>
      scala.io.Source.fromFile(f).getLines().map { s =>
        val t = s.split(" ")
        (t(0).toInt, t(1))
      }.toMap
    }

    sc
      .tableRowJsonFile(args.getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
      .map(row => new Instant(row.getLong("timestamp") * 1000L).toDateTime.getMonthOfYear)
      .countByValue
      // distributed cache available inside a transform
      .map(kv => dc().getOrElse(kv._1, "unknown") + " " + kv._2)
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
