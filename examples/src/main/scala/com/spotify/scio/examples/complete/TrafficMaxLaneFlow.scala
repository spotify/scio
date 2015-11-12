package com.spotify.scio.examples.complete

import com.google.api.services.bigquery.model.{TableSchema, TableFieldSchema}
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleOptions
import org.joda.time.{Duration, Instant}
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._

case class LaneInfo(stationId: String, lane: String, direction: String, freeway: String, recordedTimestamp: String,
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
  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val sc = ScioContext(opts)

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
      new TableFieldSchema().setName("recorded_timestamp").setType("STRING")).asJava)

    // set up example wiring
    opts.setBigQuerySchema(schema)
    val dataflowUtils = new DataflowExampleUtils(opts)
    dataflowUtils.setup()

    val input = if (opts.isStreaming) {
      sc.pubsubTopic(opts.getPubsubTopic)
    } else {
      sc.textFile(args("inputFile"))
    }

    val windowDuration = args.getOrElse("windowDuration", "60").toInt
    val windowSlideEvery = args.getOrElse("windowSlideEvery", "5").toInt

    input
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
          case _: Throwable => Seq.empty
        }
      }
      .timestampBy(v => new Instant(DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss").parseMillis(v._2.recordedTimestamp)))
      .withSlidingWindows(Duration.standardMinutes(windowDuration), Duration.standardMinutes(windowSlideEvery))
      .maxByKey(Ordering.by(_.laneFlow))
      .values
      .withTimestamp()
      .map { kv =>
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
      args.optional("inputFile").foreach { inputFile =>
        dataflowUtils.runInjectorPipeline(inputFile, opts.getPubsubTopic)
      }
    }

    // CTRL-C to cancel the streaming pipeline
    dataflowUtils.waitToFinish(result.internal)
  }
}
