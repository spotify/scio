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
import magnolify.beam.logical.nanos._
import org.joda.time.Instant

// ## Iceberg IO example

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.IcebergExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// FIXME
object IcebergExample {
  /*
  --------------------------------------------------------------------------------------------->
 partition    | row(__PARTITIONTIME timestamp(6))                                                                                                                                                                                                                                                                                                                                     >
 record_count | BigDecimal                                                                                                                                                                                                                                                                                                                                                                >
 file_count   | BigDecimal                                                                                                                                                                                                                                                                                                                                                                >
 total_size   | BigDecimal                                                                                                                                                                                                                                                                                                                                                                >
 data         | row(timestamp row(min timestamp(6), max timestamp(6), null_count BigDecimal, nan_count BigDecimal), country_code row(min varchar, max varchar, null_count BigDecimal, nan_count BigDecimal), url row(min varchar, max varchar, null_count BigDecimal, nan_count BigDecimal), project row(min varchar, max varchar, null_count BigDecimal, nan_count BigDecimal), tls_protocol
   */
  case class FileDownloads(
    record_count: BigDecimal,
    file_count: BigDecimal,
    total_size: BigDecimal,
    data: Data
  )
  case class Data(timestamp: Timestamp)
  case class Timestamp(min: Instant, max: Instant, null_count: BigDecimal, nan_count: BigDecimal)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.iceberg[FileDownloads](
      // TODO quoted things don't work via iceberg/hive
      "",
      "",
      Map(
        "type" -> "hive",
        "uri" -> "",
        "warehouse" -> ""
      )
    ).debug(prefix = "FileDownload: ")

    sc.run()
  }
}
