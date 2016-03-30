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

package com.spotify.scio.examples.complete

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.{ExampleData, ExampleOptions}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

case class LaneInfo(stationId: String, lane: String, direction: String, freeway: String,
                    recordedTimestamp: String,
                    laneFlow: Int, laneAO: Double, laneAS: Double, totalFlow: Int)

/*
SBT
runMain
  com.spotify.scio.examples.complete.TrafficMaxLaneFlow
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --streaming=true
  --pubsubTopic=projects/[PROJECT]/topics/traffic_max_lane_flow
  --inputFile=gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv
  --bigQueryDataset=[DATASET]
  --bigQueryTable=[TABLE]
*/

object TrafficMaxLaneFlow {

  val schema = new TableSchema().setFields(List(
    new TableFieldSchema().setName("station_id").setType("STRING"),
    new TableFieldSchema().setName("direction").setType("STRING"),
    new TableFieldSchema().setName("freeway").setType("STRING"),
    new TableFieldSchema().setName("lane_max_flow").setType("INTEGER"),
    new TableFieldSchema().setName("lane").setType("STRING"),
    new TableFieldSchema().setName("avg_occ").setType("FLOAT"),
    new TableFieldSchema().setName("avg_speed").setType("FLOAT"),
    new TableFieldSchema().setName("total_flow").setType("INTEGER"),
    new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"),
    new TableFieldSchema().setName("recorded_timestamp").setType("STRING")
  ).asJava)

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    if (opts.isStreaming) {
      opts.setRunner(classOf[DataflowPipelineRunner])
    }
    opts.setBigQuerySchema(schema)
    val dataflowUtils = new DataflowExampleUtils(opts)
    dataflowUtils.setup()

    // arguments
    val inputFile = args.optional("inputFile")
    val windowDuration = args.getOrElse("windowDuration", "60").toInt
    val windowSlideEvery = args.getOrElse("windowSlideEvery", "5").toInt

    val sc = ScioContext(opts)

    // initialize input
    val input = if (opts.isStreaming) {
      sc.pubsubTopic(opts.getPubsubTopic)
    } else {
      sc.textFile(inputFile.getOrElse(ExampleData.TRAFFIC))
    }

    val stream = input
      .flatMap { s =>
        val items = s.split(",")
        try {
          val (timestamp, stationId, freeway, direction) = (items(0), items(1), items(2), items(3))
          val totalFlow = items(7).toInt
          (1 to 8).map { i =>
            val laneFlow = items(6 + 5 * i).toInt
            val laneAvgOccupancy = items(7 + 5 * i).toDouble
            val laneAvgSpeed = items(8 + 5 * i).toDouble
            (stationId, LaneInfo(
              stationId, "lane" + i, direction, freeway, timestamp,
              laneFlow, laneAvgOccupancy, laneAvgSpeed, totalFlow))
          }
        } catch {
          case NonFatal(_) => Seq.empty
        }
      }

    val p = if (opts.isStreaming) {
      stream
    } else {
      lazy val formatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
      stream
        .timestampBy(v => new Instant(formatter.parseMillis(v._2.recordedTimestamp)))
    }

    p
      .withSlidingWindows(
        Duration.standardMinutes(windowDuration),
        Duration.standardMinutes(windowSlideEvery))
      .maxByKey(Ordering.by(_.laneFlow))
      .values
      .withTimestamp
      .map { kv =>  // (lane flow, timestamp)
        val (l, ts) = kv
        TableRow(
          "station_id" -> l.stationId,
          "direction" -> l.direction,
          "freeway" -> l.freeway,
          "lane_max_flow" -> l.laneFlow,
          "lane" -> l.lane,
          "avg_occ" -> l.laneAO,
          "avg_speed" -> l.laneAS,
          "total_flow" -> l.totalFlow,
          "recorded_timestamp" -> l.recordedTimestamp,
          "window_timestamp" -> ts.toString)
      }
      .saveAsBigQuery(ExampleOptions.bigQueryTable(opts), schema)

    val result = sc.close()

    // set up Pubsub topic from input file in an injector pipeline
    if (opts.isStreaming) {
      inputFile.foreach { f =>
        dataflowUtils.runInjectorPipeline(f, opts.getPubsubTopic)
      }
    }

    // CTRL-C to cancel the streaming pipeline
    dataflowUtils.waitToFinish(result.internal)
  }
  // scalastyle:on method.length

}
