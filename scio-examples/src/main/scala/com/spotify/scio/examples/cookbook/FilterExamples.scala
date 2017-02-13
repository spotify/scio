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
import com.spotify.scio.bigquery._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

import scala.collection.JavaConverters._

case class Record(year: Long, month: Long, day: Long, meanTemp: Double)

/*
SBT
runMain
  com.spotify.scio.examples.cookbook.FilterExamples
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --gcpTempLocation=gs://[BUCKET]/path/to/staging
  --output=[DATASET].filter_examples
*/

object FilterExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("year").setType("INTEGER"),
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("day").setType("INTEGER"),
      new TableFieldSchema().setName("mean_temp").setType("FLOAT")
    ).asJava)

    val monthFilter = args.int("monthFilter", 7)

    val pipe = sc.bigQueryTable(args.getOrElse("input", ExampleData.WEATHER_SAMPLES_TABLE))
      .map { row =>
        val year = row.getLong("year")
        val month = row.getLong("month")
        val day = row.getLong("day")
        val meanTemp = row.getDouble("mean_temp")
        Record(year, month, day, meanTemp)
      }

    val globalMeanTemp = pipe.map(_.meanTemp).mean

    pipe
      .filter(_.month == monthFilter)
      .cross(globalMeanTemp)
      .filter(kv => kv._1.meanTemp < kv._2)
      .keys
      .map { r =>
        TableRow("year" -> r.year, "month" -> r.month, "day" -> r.day, "mean_temp" -> r.meanTemp)
      }
      .saveAsBigQuery(args("output"), schema, WRITE_TRUNCATE, CREATE_IF_NEEDED)

    sc.close()
  }
}
