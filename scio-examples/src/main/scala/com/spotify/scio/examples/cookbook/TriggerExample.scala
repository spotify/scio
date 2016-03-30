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

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.dataflow.examples.common.{DataflowExampleUtils, PubsubFileInjector}
import com.google.cloud.dataflow.examples.cookbook.TriggerExample.InsertDelays
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.windowing._
import com.google.cloud.dataflow.sdk.transforms.{IntraBundleParallelization, ParDo}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleOptions
import com.spotify.scio.values.{WindowOptions, WindowedValue}
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._
import scala.util.Try

/*
SBT
runMain
  com.spotify.scio.examples.cookbook.TriggerExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --pubsubTopic=projects/[PROJECT]/topics/traffic_routes
  --input=gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv
  --bigQueryDataset=[DATASET]
  --bigQueryTable=[TABLE]
*/

object TriggerExample {

  val PUBSUB_TIMESTAMP_LABEL_KEY = "timestamp_ms"

  val schema = new TableSchema().setFields(List(
    new TableFieldSchema().setName("trigger_type").setType("STRING"),
    new TableFieldSchema().setName("freeway").setType("STRING"),
    new TableFieldSchema().setName("total_flow").setType("INTEGER"),
    new TableFieldSchema().setName("number_of_records").setType("INTEGER"),
    new TableFieldSchema().setName("window").setType("STRING"),
    new TableFieldSchema().setName("isFirst").setType("BOOLEAN"),
    new TableFieldSchema().setName("isLast").setType("BOOLEAN"),
    new TableFieldSchema().setName("timing").setType("STRING"),
    new TableFieldSchema().setName("event_time").setType("TIMESTAMP"),
    new TableFieldSchema().setName("processing_time").setType("TIMESTAMP")
    ).asJava)

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    opts.setStreaming(true)
    opts.setRunner(classOf[DataflowPipelineRunner])
    opts.setBigQuerySchema(schema)
    val dataflowUtils = new DataflowExampleUtils(opts)
    dataflowUtils.setup()

    // arguments
    val input = args.optional("input")
    val windowDuration = args.int("windowDuration", 30)

    val sc = ScioContext(opts)

    val flowInfo = sc.pubsubTopic(opts.getPubsubTopic, timestampLabel = PUBSUB_TIMESTAMP_LABEL_KEY)
      .flatMap { s =>
        val laneInfo = s.split(",")
        if (laneInfo(0).equals("timestamp") || laneInfo.length < 48) {
          Seq.empty
        } else {
          Try {
            val freeway = laneInfo(2)
            val totalFlow = laneInfo(7).toInt
            if (totalFlow > 0) {
              Seq((freeway, totalFlow))
            } else {
              Seq.empty
            }
          }.getOrElse(Seq.empty)
        }
      }

    def compute(triggerType: String, windowOptions: WindowOptions[IntervalWindow]): Unit = {
      flowInfo
        .withFixedWindows(Duration.standardMinutes(windowDuration), options = windowOptions)
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
          val newValue = TableRow(
            "trigger_type" -> triggerType,
            "freeway" -> key,
            "total_flow" -> sum,
            "number_of_records" -> numberOfRecords,
            "window" -> wv.window.toString,
            "isFirst" -> wv.pane.isFirst,
            "isLast" -> wv.pane.isLast,
            "timing" -> wv.pane.getTiming.toString,
            "event_time" -> wv.timestamp.toString,
            "processing_time" -> Instant.now().toString)
          wv.withValue(newValue)
        }
        .toSCollection
        .saveAsBigQuery(ExampleOptions.bigQueryTable(opts), schema)
    }

    val ONE_MINUTE = Duration.standardMinutes(1)
    val FIVE_MINUTES = Duration.standardMinutes(5)
    val ONE_DAY = Duration.standardDays(1)

    compute("default",
      WindowOptions(
        allowedLateness = Duration.ZERO,
        trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
        accumulationMode = DISCARDING_FIRED_PANES))

    compute("withAllowedLateness",
      WindowOptions(
        allowedLateness = ONE_DAY,
        trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
        accumulationMode = DISCARDING_FIRED_PANES))

    compute("speculative",
      WindowOptions(
        allowedLateness = ONE_DAY,
        trigger = Repeatedly.forever(
          AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(ONE_MINUTE)),
        accumulationMode = ACCUMULATING_FIRED_PANES))

    compute("sequential",
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

    val result = sc.close()

    // set up Pubsub topic from input file in an injector pipeline
    input.foreach { f =>
      dataflowUtils.runInjectorPipeline(runInjector(opts, f))
    }

    // CTRL-C to cancel the streaming pipeline
    dataflowUtils.waitToFinish(result.internal)
  }
  // scalastyle:on method.length

  private def runInjector(opts: ExampleOptions, input: String): Pipeline = {
    val copiedOpts = opts.cloneAs(classOf[DataflowPipelineOptions])
    copiedOpts.setStreaming(false)
    copiedOpts.setNumWorkers(opts.getInjectorNumWorkers)
    copiedOpts.setJobName(opts.getJobName + "-injector")
    val pipeline = Pipeline.create(copiedOpts)
    pipeline
      .apply(TextIO.Read.named("ReadMyFile").from(input))
      .apply(ParDo.named("InsertRandomDelays").of(new InsertDelays))
      .apply(IntraBundleParallelization.of(PubsubFileInjector
        .withTimestampLabelKey(PUBSUB_TIMESTAMP_LABEL_KEY)
        .publish(opts.getPubsubTopic))
        .withMaxParallelism(20))
    pipeline
  }

}
