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

package com.spotify.scio.examples.cookbook

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.streaming._
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing._
import org.joda.time.{DateTimeConstants, Duration, Instant}

import scala.util.Try

/*
SBT
runMain
  com.spotify.scio.examples.cookbook.TriggerExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv
  --output=[DATASET].trigger_example
*/

object TriggerExample {

  @BigQueryType.toTable
  case class Record(trigger_type: String, freeway: String, total_flow: Long,
                    number_of_records: Long, window: String, is_first: Boolean, is_last: Boolean,
                    timing: String, event_time: Instant, processing_time: Instant)

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    opts.as(classOf[StreamingOptions]).setStreaming(true)
    val exampleUtils = new ExampleUtils(opts)

    // arguments
    val input = args.getOrElse("input", ExampleData.TRAFFIC)
    val windowDuration = Duration.standardMinutes(args.int("windowDuration", 30))

    val sc = ScioContext(opts)

    val flowInfo = extractFlowInfo(insertDelays(sc.textFile(input)))

    def compute(triggerType: String,
                windowOptions: WindowOptions): SCollection[Record] = {
      val f = flowInfo.withFixedWindows(windowDuration, options = windowOptions)
      totalFlow(f, triggerType)
    }

    val ONE_MINUTE = Duration.standardMinutes(1)
    val FIVE_MINUTES = Duration.standardMinutes(5)
    val ONE_DAY = Duration.standardDays(1)

    val defaultTriggerResults = compute("default",
      WindowOptions(
        allowedLateness = Duration.ZERO,
        trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
        accumulationMode = DISCARDING_FIRED_PANES))

    val withAllowedLatenessResults = compute("withAllowedLateness",
      WindowOptions(
        allowedLateness = ONE_DAY,
        trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
        accumulationMode = DISCARDING_FIRED_PANES))

    val speculativeResults = compute("speculative",
      WindowOptions(
        allowedLateness = ONE_DAY,
        trigger = Repeatedly.forever(
          AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(ONE_MINUTE)),
        accumulationMode = ACCUMULATING_FIRED_PANES))

    val sequentialResults = compute("sequential",
      WindowOptions(
        allowedLateness = ONE_DAY,
        trigger = AfterEach.inOrder(
          Repeatedly.forever(
            AfterProcessingTime
              .pastFirstElementInPane()
              .plusDelayOf(ONE_MINUTE))
            .orFinally(
              AfterWatermark.pastEndOfWindow()),
          Repeatedly.forever(
            AfterProcessingTime
              .pastFirstElementInPane()
              .plusDelayOf(FIVE_MINUTES))),
        accumulationMode = ACCUMULATING_FIRED_PANES))

    SCollection.unionAll(Seq(
      defaultTriggerResults,
      withAllowedLatenessResults,
      speculativeResults,
      sequentialResults)).saveAsTypedBigQuery(args("output"))

    val result = sc.close()
    exampleUtils.waitToFinish(result.internal)
  }
  // scalastyle:on method.length

  private val THRESHOLD = 0.001
  private val MIN_DELAY = 1
  private val MAX_DELAY = 100

  def insertDelays(input: SCollection[String]): SCollection[String] = input.timestampBy { _ =>
    val timestamp = Instant.now()
    if (Math.random() < THRESHOLD) {
      val range = MAX_DELAY - MIN_DELAY
      val delayInMinutes = (Math.random() * range) + MIN_DELAY
      val delayInMillis = DateTimeConstants.MILLIS_PER_MINUTE * delayInMinutes
      new Instant(timestamp.getMillis - delayInMillis)
    } else {
      timestamp
    }
  }

  def extractFlowInfo(input: SCollection[String]): SCollection[(String, Int)] =
    input
      .flatMap { s =>
        val laneInfo = s.split(",")
        if (laneInfo(0).equals("timestamp") || laneInfo.length < 48) {
          // header row or invalid input
          Nil
        } else {
          Try {
            val freeway = laneInfo(2)
            val totalFlow = laneInfo(7).toInt
            if (totalFlow > 0) {
              Seq((freeway, totalFlow))
            } else {
              Seq.empty
            }
          }.getOrElse(Nil)
        }
      }

  def totalFlow(flowInfo: SCollection[(String, Int)], triggerType: String): SCollection[Record] =
    flowInfo
      .groupByKey
      .toWindowed
      .map { wv =>
        val (key, values) = wv.value
        var sum = 0
        var numberOfRecords = 0L
        values.foreach { v =>
          sum += v
          numberOfRecords += 1
        }
        val newValue = Record(
          triggerType, key, sum, numberOfRecords, wv.window.toString,
          wv.pane.isFirst, wv.pane.isLast, wv.pane.getTiming.toString,
          wv.timestamp, Instant.now())
        wv.withValue(newValue)
      }
      .toSCollection

}
