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
import org.apache.beam.sdk.schemas.utils.YamlUtils

import scala.jdk.CollectionConverters._

case class Fake(a: String, b: Int)

case class FakeNested(a: String, b: Int, c: Fake)

class IcebergIOTest extends ScioIOSpec {
  "IcebergIO" should "produce snake_case config maps" in {
    val io = IcebergIO[Fake]("tableName", Some("catalogName"))

    val reads: Seq[IcebergIO.ReadParam] = List(
      IcebergIO.ReadParam(),
      IcebergIO.ReadParam(
        Map.empty,
        Map.empty,
        ""
      ),
      IcebergIO.ReadParam(
        Map("catalogProp1" -> "catalogProp1Value"),
        Map("configProp1" -> "configProp1Value", "configProp2" -> "configProp2Value"),
        "id > 10"
      )
    )
    val writes: Seq[IcebergIO.WriteParam] = List(
      IcebergIO.WriteParam(),
      IcebergIO.WriteParam(
        Map.empty,
        Map.empty,
        Nil,
        Nil,
        None,
        None
      ),
      IcebergIO.WriteParam(
        Map("catalogProp1" -> "catalogProp1Value"),
        Map("configProp1" -> "configProp1Value", "configProp2" -> "configProp2Value"),
        List("sortField1"),
        List("partField1"),
        Some(10),
        Some(100)
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
      // reads
      "filter",
      // writes
      "write_properties",
      "sort_fields",
      "partition_fields",
      "triggering_frequency_seconds",
      "direct_write_byte_limit"
    )
    assert(configs.flatMap(_.keys).toSet.intersect(expectedKeys) == expectedKeys)

    // don't throw
    configs.map(ManagedIO.convertConfig)
  }

  it should "produce Java iterables compatible with Beam ManagedIO's yaml-parsing library" in {
    val config = ManagedIO.convertConfig(
      IcebergIO[Fake]("tableName", Some("catalogName")).config(
        IcebergIO.ReadParam(
          Map("a" -> "b"),
          Map("c" -> "d", "e" -> "f")
        )
      )
    )

    assert(
      config == Map(
        "config_properties" -> Map("c" -> "d", "e" -> "f").asJava,
        "catalog_properties" -> Map("a" -> "b").asJava,
        "table" -> "tableName",
        "catalog_name" -> "catalogName",
        "keep" -> List("a", "b").asJava
      ).asJava
    )

    // invoked by Beam's Managed transform internally; shouldn't throw
    YamlUtils.yamlStringFromMap(config)
  }

  it should "infer correct read config for Magnolify RowTypes" in {
    val io = IcebergIO[FakeNested]("tableName", Some("catalogName"))

    val readParam = IcebergIO.ReadParam(
      Map("a" -> "b"),
      Map("c" -> "d", "e" -> "f"),
      "id > 10"
    )

    val managedConfig: Map[String, AnyRef] = io.config(readParam)

    managedConfig should contain only (
      "config_properties" -> Map("c" -> "d", "e" -> "f"),
      "filter" -> "id > 10",
      "catalog_properties" -> Map("a" -> "b"),
      "table" -> "tableName",
      "keep" -> List("a", "b", "c.a", "c.b"),
      "catalog_name" -> "catalogName"
    )
  }

  it should "infer correct write config for Magnolify RowTypes" in {
    val io = IcebergIO[FakeNested]("tableName", Some("catalogName"))

    val writeParam = IcebergIO.WriteParam(
      Map("a" -> "b"),
      Map("c" -> "d", "e" -> "f"),
      List("col1", "col2"),
      List("partCol1"),
      extraConfigProperties = Map(
        "distribution_mode" -> "hash",
        "autosharding" -> (true: java.lang.Boolean)
      )
    )

    val managedConfig: Map[String, AnyRef] = io.config(writeParam)

    managedConfig should contain only (
      "write_properties" -> Map("c" -> "d", "e" -> "f"),
      "sort_fields" -> List("col1", "col2"),
      "partition_fields" -> List("partCol1"),
      "catalog_properties" -> Map("a" -> "b"),
      "distribution_mode" -> "hash",
      "autosharding" -> true,
      "table" -> "tableName",
      "catalog_name" -> "catalogName"
    )
  }
}
