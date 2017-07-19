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

package com.spotify.scio.examples

import java.nio.channels.Channels
import java.util.concurrent.ThreadLocalRandom

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.util.MimeTypes
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{Duration, Instant}

/*
SBT
runMain
  com.spotify.scio.examples.WindowedWordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/windowed_wordcount
*/

object WindowedWordCount {

  private val WINDOW_SIZE = 10L
  private val formatter = ISODateTimeFormat.hourMinute

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    FileSystems.setDefaultPipelineOptions(sc.options)

    val input = args.getOrElse("input", ExampleData.KING_LEAR)
    val windowSize = Duration.standardMinutes(args.long("windowSize", WINDOW_SIZE))
    val minTimestamp = args.long("minTimestampMillis", System.currentTimeMillis())
    val maxTimestamp = args.long(
      "maxTimestampMillis", minTimestamp + Duration.standardHours(1).getMillis)

    sc
      .textFile(input)
      .timestampBy {
        _ => new Instant(ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp))
      }
      .withFixedWindows(windowSize)  // apply windowing logic
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .withWindow[IntervalWindow]
      .swap
      .groupByKey
      .map { case (w, vs) =>
        val outputShard = "%s-%s-%s".format(
          args("output"), formatter.print(w.start()), formatter.print(w.end()))
        val resourceId = FileSystems.matchNewResource(outputShard, false)
        val out = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.TEXT))
        vs.foreach { case (k, v) => out.write(s"$k: $v\n".getBytes) }
        out.close()
      }

    sc.close()
  }

}
