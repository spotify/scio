/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import com.spotify.scio.managed._
import com.spotify.scio.values.SCollection
import magnolify.beam._
import org.apache.beam.sdk.managed.Managed
import org.apache.beam.sdk.values.Row

// Example: Beam's Managed IO

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.ManagedExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --table=[TABLE] --catalogName=[CATALOG] --catalogType=[CATALOG TYPE]
// --catalogUri=[CATALOG URI] --catalogWarehouse=[CATALOG WAREHOUSE]
// --output=[OUTPUT PATH]"`
object ManagedExample {

  case class Record(a: Int, b: String)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val config: Map[String, Object] = Map(
      "table" -> args("table"),
      "catalog_name" -> args("catalogName"),
      "catalog_properties" ->
        Map(
          "type" -> args("catalogType"),
          "uri" -> args("catalogUri"),
          "warehouse" -> args("catalogWarehouse")
        )
    )

    val rt = RowType[Record]
    // Provide an implicit coder for Row with the schema derived from Record case class
    implicit val recordRowCoder: Coder[Row] = Coder.row(rt.schema)

    // Read beam Row instances from iceberg
    val records: SCollection[Record] = sc
      .managed(
        Managed.ICEBERG,
        // Schema derived from the Record case class
        rt.schema,
        config
      )
      // Convert the Row instance to a Record
      .map(rt.apply)

    records
      .map(r => r.copy(a = r.a + 1))
      // Convert the Record to a Row
      .map(rt.apply)
      // Save Row instances to Iceberg
      .saveAsManaged(Managed.ICEBERG, config)

    sc.run()
  }
}
