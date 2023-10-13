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

// Example: Traffic routes based on traffic sensor data
// Usage

// `sbt "runMain com.spotify.scio.examples.complete.TrafficRoutes
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --input=gs://apache-beam-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv
// --output=[DATASET].traffic_routes"`
package com.spotify.scio.examples.complete

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.extra.csv._
import kantan.csv.HeaderDecoder
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Duration, Instant}

object TrafficRoutes {
  case class StationSpeed(
    stationType: String,
    stationId: String,
    avgSpeed: Double,
    timestamp: String
  )

  private val fmt = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
  implicit val csvDecoderStationSpeed: HeaderDecoder[StationSpeed] = HeaderDecoder.decoder(
    "Station Type",
    "Station ID",
    "Average Speed",
    "Timestamp"
  )(StationSpeed.apply)

  case class RouteInfo(route: String, avgSpeed: Double, slowdownEvent: Boolean)

  @BigQueryType.toTable
  case class Record(
    route: String,
    avg_speed: Double,
    slowdown_event: Boolean,
    window_timestamp: Instant
  )

  private val sdStations =
    Map("1108413" -> "SDRoute1", "1108699" -> "SDRoute2", "1108702" -> "SDRoute3")

  def main(cmdlineArgs: Array[String]): Unit = {
    // set up example wiring
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val exampleUtils = new ExampleUtils(opts)

    // arguments
    val input = args.getOrElse("input", ExampleData.TRAFFIC)
    val windowDuration = args.long("windowDuration", 3)
    val windowSlideEvery = args.long("windowSlideEvery", 1)

    val sc = ScioContext(opts)

    sc.csvFile[StationSpeed](input)
      .flatMap { ss =>
        for {
          route <- sdStations.get(ss.stationId) if ss.stationType == "ML"
        } yield route -> ss
      }
      .timestampBy { case (_, ss) => new Instant(fmt.parseMillis(ss.timestamp)) }
      .withSlidingWindows(
        Duration.standardMinutes(windowDuration),
        Duration.standardMinutes(windowSlideEvery)
      )
      .groupByKey
      .map { case (route, speeds) =>
        var speedSum = 0.0
        var speedCount = 0
        var speedups = 0
        var slowdowns = 0
        val prevSpeeds = scala.collection.mutable.Map[String, Double]()

        speeds.toList.sortBy(_.timestamp).foreach { i =>
          speedSum += i.avgSpeed
          speedCount += 1
          prevSpeeds.get(i.stationId).foreach { s =>
            if (s < i.avgSpeed) {
              speedups += 1
            } else {
              slowdowns += 1
            }
          }
          prevSpeeds(i.stationId) = i.avgSpeed
        }
        val speedAvg = speedSum / speedCount
        val slowdownEvent = slowdowns >= 2 * speedups
        RouteInfo(route, speedAvg, slowdownEvent)
      }
      .withTimestamp // explodes internal timestamp
      .map { case (r, ts) =>
        Record(r.route, r.avgSpeed, r.slowdownEvent, ts)
      }
      .saveAsTypedBigQueryTable(Table.Spec(args("output")))

    val result = sc.run()
    exampleUtils.waitToFinish(result.pipelineResult)
  }
}
