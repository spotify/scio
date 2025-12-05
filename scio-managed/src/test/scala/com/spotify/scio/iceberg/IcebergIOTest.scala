/*
 * Copyright 2025 Spotify AB
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

package com.spotify.scio.iceberg

import com.spotify.scio.managed.ManagedIO
import com.spotify.scio.testing.ScioIOSpec

case class Fake(a: String, b: Int)

class IcebergIOTest extends ScioIOSpec {
  "IcebergIO" should "produce snake_case config maps" in {
    val io = IcebergIO[Fake]("tableName", Some("catalogName"))

    val reads: Seq[IcebergIO.ReadParam] = List(
      IcebergIO.ReadParam(),
      IcebergIO.ReadParam(
        Map.empty,
        Map.empty,
        List.empty,
        List.empty,
        ""
      ),
      IcebergIO.ReadParam(
        Map("catalogProp1" -> "catalogProp1Value"),
        Map("configProp1" -> "configProp1Value", "configProp2" -> "configProp2Value"),
        List("keep1", "keep2", "keep3"),
        List("drop1", "drop2", "drop3"),
        "id > 10"
      )
    )
    val writes: Seq[IcebergIO.WriteParam] = List(
      IcebergIO.WriteParam(),
      IcebergIO.WriteParam(
        Map.empty,
        Map.empty,
        null.asInstanceOf[Int],
        null.asInstanceOf[Int],
        List.empty,
        List.empty,
        ""
      ),
      IcebergIO.WriteParam(
        Map("catalogProp1" -> "catalogProp1Value"),
        Map("configProp1" -> "configProp1Value", "configProp2" -> "configProp2Value"),
        10,
        100,
        List("keep1", "keep2", "keep3"),
        List("drop1", "drop2", "drop3"),
        "only"
      )
    )

    val configs: Seq[Map[String, AnyRef]] = reads.map(io.config(_)) ++ writes.map(io.config(_))
    val expectedKeys = Set(
      // common
      "table",
      "catalog_name",
      "catalog_properties",
      "config_properties",
      "keep",
      "drop",
      // reads
      "filter",
      // writes
      "triggering_frequency_seconds",
      "direct_write_byte_limit",
      "only"
    )
    assert(configs.flatMap(_.keys).toSet.intersect(expectedKeys) == expectedKeys)

    // don't throw
    configs.map(ManagedIO.convertConfig)
  }
}
