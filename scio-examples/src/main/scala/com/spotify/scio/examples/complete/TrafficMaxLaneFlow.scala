/*
 * Copyright 2019 Spotify AB.
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

// `sbt "runMain com.spotify.scio.examples.complete.TrafficMaxLaneFlow
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --input=gs://apache-beam-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv
// --output=[DATASET].traffic_max_lane_flow"`
package com.spotify.scio.examples.complete

import cats.implicits._
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.extra.csv._
import kantan.csv.RowDecoder
import org.apache.beam.examples.common.ExampleUtils
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Duration, Instant}

object TrafficMaxLaneFlow {
  case class LaneInfo(
    stationId: String,
    lane: String,
    direction: String,
    freeway: String,
    recordedTimestamp: String,
    flow: Int,
    avgOcc: Double,
    avgSpeed: Double,
    totalFlow: Int
  )

  private val SamplesOffset = 5
  implicit val csvDecoderLaneInfo: RowDecoder[List[LaneInfo]] = RowDecoder.from { ss =>
    for {
      timestamp <- RowDecoder.decodeCell[String](ss, 0)
      stationId <- RowDecoder.decodeCell[String](ss, 1)
      freeway <- RowDecoder.decodeCell[String](ss, 2)
      direction <- RowDecoder.decodeCell[String](ss, 3)
      totalFlow <- RowDecoder.decodeCell[Int](ss, 7)
      laneInfos <- (1 to 8)
        .map { i =>
          val offset = i * SamplesOffset
          for {
            laneFlow <- RowDecoder.decodeCell[Int](ss, offset + 6)
            laneAvgOccupancy <- RowDecoder.decodeCell[Double](ss, offset + 7)
            laneAvgSpeed <- RowDecoder.decodeCell[Double](ss, offset + 8)
          } yield LaneInfo(
            stationId,
            "lane" + i,
            direction,
            freeway,
            timestamp,
            laneFlow,
            laneAvgOccupancy,
            laneAvgSpeed,
            totalFlow
          )
        }
        .toList
        .sequence
    } yield laneInfos
  }

  @BigQueryType.toTable
  case class Record(
    station_id: String,
    direction: String,
    freeway: String,
    lane_max_flow: Int,
    lane: String,
    avg_occ: Double,
    avg_speed: Double,
    totalFlow: Int,
    recorded_timestamp: String,
    window_timestamp: Instant
  )

  val fmt = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")

  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val exampleUtils = new ExampleUtils(sc.options)
    exampleUtils.setup()

    // arguments
    val input = args.getOrElse("input", ExampleData.TRAFFIC)
    val windowDuration = args.long("windowDuration", 60)
    val windowSlideEvery = args.long("windowSlideEvery", 5)

    sc.csvFile[List[LaneInfo]](input)
      .flatten
      .timestampBy(li => new Instant(fmt.parseMillis(li.recordedTimestamp)))
      .withSlidingWindows(
        Duration.standardMinutes(windowDuration),
        Duration.standardMinutes(windowSlideEvery)
      )
      .keyBy(_.stationId)
      .maxByKey(Ordering.by(_.flow))
      .values
      .withTimestamp
      .map { case (l, ts) => // (lane flow, timestamp)
        Record(
          l.stationId,
          l.direction,
          l.freeway,
          l.flow,
          l.lane,
          l.avgOcc,
          l.avgSpeed,
          l.totalFlow,
          l.recordedTimestamp,
          ts
        )
      }
      .saveAsTypedBigQueryTable(Table.Spec(args("output")))

    val result = sc.run()
    exampleUtils.waitToFinish(result.pipelineResult)
  }
}
