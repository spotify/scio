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

// Example: Minimal BeamSQL Example
// Usage:

// `sbt runMain "com.spotify.scio.examples.BeamSqlExample
//  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]"`
package com.spotify.scio.examples

import com.spotify.scio._

import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import com.spotify.scio.coders.Coder
import org.slf4j.LoggerFactory
import java.lang.{Integer => jInt, String => jString, Double => jDouble}

object BeamSqlExample {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private def log(name: String): Row => Row = { input =>
    logger.debug(s"$name: ${input.getValues()}")
    input
  }

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, _) = ContextAndArgs(cmdlineArgs)

    // define the input row format
    val schema =
      Schema.builder().addInt32Field("c1").addStringField("c2").addDoubleField("c3").build()
    def coderRow1 = Coder.row(schema)

    val schemaAggr = Schema.builder().addStringField("c2").addDoubleField("sum(c3)").build()
    def coderRowAggr = Coder.row(schemaAggr)

    val rows =
      List[(jInt, jString, jDouble)]((1, "row", 1.0), (2, "row", 2.0), (3, "row", 3.0)).map {
        case (a, b, c) =>
          Row.withSchema(schema).addValues(a, b, c).build()
      }

    // Case 1. run a simple SQL query over input `PCollection` with `SqlTransform.query`
    sc.parallelize(rows)(coderRow1)
      .map(log("PCOLLECTION"))(coderRow1)
      .applyTransform(SqlTransform.query("select c1, c2, c3 from PCOLLECTION where c1 > 1"))(
        coderRow1)
      .applyTransform(SqlTransform.query("select c2, sum(c3) from PCOLLECTION group by c2"))(
        coderRowAggr)
      .map(log("CASE1_RESULT"))(coderRowAggr)

    // Close the context and execute the pipeline
    sc.close()
  }
}
