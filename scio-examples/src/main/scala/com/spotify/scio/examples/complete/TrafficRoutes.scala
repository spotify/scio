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
import org.joda.time.{Duration, Instant}
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

case class StationSpeed(stationId: String, avgSpeed: Double, timestamp: Long)
case class RouteInfo(route: String, avgSpeed: Double, slowdownEvent: Boolean)

/*
SBT
runMain
  com.spotify.scio.examples.complete.TrafficRoutes
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --streaming=true
  --pubsubTopic=projects/[PROJECT]/topics/traffic_routes
  --inputFile=gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv
  --bigQueryDataset=[DATASET]
  --bigQueryTable=[TABLE]
*/

object TrafficRoutes {

  val schema = new TableSchema().setFields(List(
    new TableFieldSchema().setName("route").setType("STRING"),
    new TableFieldSchema().setName("avg_speed").setType("FLOAT"),
    new TableFieldSchema().setName("slowdown_event").setType("BOOLEAN"),
    new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP")
  ).asJava)

  val sdStations = Map("1108413" -> "SDRoute1", "1108699" -> "SDRoute2", "1108702" -> "SDRoute3")

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
    val windowDuration = args.getOrElse("windowDuration", "3").toInt
    val windowSlideEvery = args.getOrElse("windowSlideEvery", "1").toInt

    val sc = ScioContext(opts)

    val input = if(opts.isStreaming) {
      sc.pubsubTopic(opts.getPubsubTopic)
    } else {
      sc.textFile(inputFile.getOrElse(ExampleData.TRAFFIC))
    }

    lazy val formatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
    val stream = input
      .flatMap { s =>
        val items = s.split(",")
        try {
          val stationType = items(4)
          val stationId = items(1)
          if (stationType == "ML" && sdStations.contains(stationId)) {
            val avgSpeed = items(9).toDouble
            val timestamp = new Instant(formatter.parseMillis(items(0)))
            Seq((sdStations(stationId), StationSpeed(stationId, avgSpeed, timestamp.getMillis)))
          } else {
            Seq()
          }
        } catch {
          case NonFatal(_) => Seq.empty
        }
      }

    val p = if (opts.isStreaming) {
      stream
    } else {
      stream.timestampBy(kv => new Instant(kv._2.timestamp))
    }

    p
      .withSlidingWindows(
        Duration.standardMinutes(windowDuration),
        Duration.standardMinutes(windowSlideEvery))
      .groupByKey
      .map { kv =>
        var speedSum = 0.0
        var speedCount = 0
        var speedups = 0
        var slowdowns = 0
        val prevSpeeds = scala.collection.mutable.Map[String, Double]()

        kv._2.toList.sortBy(_.timestamp).foreach { i =>
          speedSum += i.avgSpeed
          speedCount += 1
          prevSpeeds.get(i.stationId).foreach { s =>
            if (s < i.avgSpeed) {
              speedups +=1
            } else {
              slowdowns += 1
            }
          }
          prevSpeeds(i.stationId) = i.avgSpeed
        }
        val speedAvg = speedSum / speedCount
        val slowdownEvent = slowdowns >= 2 * speedups
        RouteInfo(kv._1, speedAvg, slowdownEvent)
      }
      .withTimestamp  // explose internal timestamp
      .map { kv =>
        val (r, ts) = kv
        TableRow(
          "avg_speed" -> r.avgSpeed,
          "slowdown_event" -> r.slowdownEvent,
          "route" -> r.route,
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
