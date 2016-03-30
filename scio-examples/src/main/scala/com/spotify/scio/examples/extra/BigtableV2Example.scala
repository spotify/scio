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

package com.spotify.scio.examples.extra

import com.google.bigtable.v1.{Mutation, Row}
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

/**
 * Bigtable V2 examples.
 *
 * This depends on native APIs in `scio-core` and `BigtableIO` form Dataflow Java SDK 1.5.0+.
 */
object BigtableV2Example {

  val FAMILY = "count"
  val QUALIFIER = "long"

  def kvToSetCell(kv: (String, Long)): (ByteString, Iterable[Mutation]) =
    BigtableV2Util.simpleSetCell(kv._1, FAMILY, QUALIFIER, kv._2.toString)

  def kvToRow(kv: (String, Long)): Row =
    BigtableV2Util.simpleRow(kv._1, FAMILY, QUALIFIER, kv._2.toString)

  def rowToKv(row: Row): (String, Long) = {
    val key = row.getKey.toStringUtf8
    val values = BigtableV2Util.simpleGetCells(row, FAMILY, QUALIFIER)
    require(values.size == 1)
    (key, values.head.toStringUtf8.toLong)
  }

  def bigtableOptions(args: Args): BigtableOptions = new BigtableOptions.Builder()
    .setProjectId(args("bigtableProjectId"))
    .setClusterId(args("bigtableClusterId"))
    .setZoneId(args("bigtableZoneId"))
    .build()
}

object BigtableV2WriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(BigtableV2Example.kvToSetCell)
      .saveAsBigtable(args("bigtableTableId"), BigtableV2Example.bigtableOptions(args))

    sc.close()
  }
}

object BigtableV2ReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.bigtable(args("bigtableTableId"), BigtableV2Example.bigtableOptions(args))
      .map(BigtableV2Example.rowToKv)
      .map(kv => kv._1 + ": " + kv._2)
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
