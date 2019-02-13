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

// Example: Traffic max lane flow computation from sensor data
// Usage:

// `sbt runMain "com.spotify.scio.examples.complete.TrafficMaxLaneFlow
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv
// --output=[DATASET].traffic_max_lane_flow"`
package com.spotify.scio.examples.complete

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Duration, Instant}

import scala.util.control.NonFatal

object TrafficMaxLaneFlow {

  case class LaneInfo(stationId: String,
                      lane: String,
                      direction: String,
                      freeway: String,
                      recordedTimestamp: String,
                      flow: Int,
                      avgOcc: Double,
                      avgSpeed: Double,
                      totalFlow: Int)

  @BigQueryType.toTable
  case class Record(station_id: String,
                    direction: String,
                    freeway: String,
                    lane_max_flow: Int,
                    lane: String,
                    avg_occ: Double,
                    avg_speed: Double,
                    totalFlow: Int,
                    recorded_timestamp: String,
                    window_timestamp: Instant)

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val exampleUtils = new ExampleUtils(opts)
    exampleUtils.setup()

    // arguments
    val input = args.getOrElse("input", ExampleData.TRAFFIC)
    val windowDuration = args.getOrElse("windowDuration", "60").toInt
    val windowSlideEvery = args.getOrElse("windowSlideEvery", "5").toInt

    val sc = ScioContext(opts)

    lazy val formatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
    sc.textFile(input)
      .flatMap { s =>
        val items = s.split(",")
        try {
          val (timestamp, stationId, freeway, direction) =
            (items(0), items(1), items(2), items(3))
          val totalFlow = items(7).toInt
          (1 to 8).map { i =>
            val laneFlow = items(6 + 5 * i).toInt
            val laneAvgOccupancy = items(7 + 5 * i).toDouble
            val laneAvgSpeed = items(8 + 5 * i).toDouble
            (stationId,
             LaneInfo(stationId,
                      "lane" + i,
                      direction,
                      freeway,
                      timestamp,
                      laneFlow,
                      laneAvgOccupancy,
                      laneAvgSpeed,
                      totalFlow))
          }
        } catch {
          case NonFatal(_) => Seq.empty
        }
      }
      .timestampBy(v => new Instant(formatter.parseMillis(v._2.recordedTimestamp)))
      .withSlidingWindows(Duration.standardMinutes(windowDuration),
                          Duration.standardMinutes(windowSlideEvery))
      .maxByKey(Ordering.by(_.flow))
      .values
      .withTimestamp
      .map {
        case (l, ts) => // (lane flow, timestamp)
          Record(l.stationId,
                 l.direction,
                 l.freeway,
                 l.flow,
                 l.lane,
                 l.avgOcc,
                 l.avgSpeed,
                 l.totalFlow,
                 l.recordedTimestamp,
                 ts)
      }
      .saveAsTypedBigQuery(args("output"))

    val result = sc.close()
    exampleUtils.waitToFinish(result.pipelineResult)
  }
  // scalastyle:on method.length

}
