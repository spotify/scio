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
import com.spotify.scio.iceberg._
import magnolify.beam._

// Example: Apache Iceberg read/write Example

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.IcebergExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --inputTable=[INPUT TABLE] --catalogName=[CATALOG NAME]
// --catalogType=[CATALOG TYPE] --catalogUri=[CATALOG URI]
// --catalogWarehouse=[CATALOG WAREHOUSE] --outputTable=[OUTPUT TABLE]"`
object IcebergExample {

  case class Record(a: Int, b: String)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Catalog configuration
    val catalogConfig = Map(
      "type" -> args("catalogType"),
      "uri" -> args("catalogUri"),
      "warehouse" -> args("catalogWarehouse")
    )

    // Derive a conversion between Record and Beam Row
    implicit val rt: RowType[Record] = RowType[Record]

    sc
      // Read Records from Iceberg
      .iceberg[Record](
        args("inputTable"),
        args.optional("catalogName").orNull,
        catalogConfig
      )
      .map(r => r.copy(a = r.a + 1))
      // Write Records to Iceberg
      .saveAsIceberg(
        args("outputTable"),
        args.optional("catalogName").orNull,
        catalogConfig
      )

    sc.run()
  }
}
