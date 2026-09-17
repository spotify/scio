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

package com.spotify.scio.iceberg

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.spotify.scio.parquet.BeamInputFile
import com.spotify.scio.testing.PipelineSpec
import magnolify.beam._
import magnolify.beam.logical.timestamp.micros._
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.types.Types.{
  BooleanType,
  IntegerType,
  NestedField,
  StringType,
  StructType,
  TimestampType
}
import org.apache.iceberg.{
  CatalogProperties,
  CatalogUtil,
  NullOrder,
  PartitionSpec,
  Schema,
  SortOrder
}
import org.apache.parquet.hadoop.ParquetFileReader
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}
import java.io.File
import java.nio.file.Files
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters._

case class Nested(d: Boolean)
case class IcebergIOITRecord(
  ts: Instant,
  tsWithoutZone: LocalDateTime,
  a: Int,
  b: String,
  c: Nested
)
object IcebergIOITRecord {
  implicit val localDateTimeRowField: RowField[LocalDateTime] =
    magnolify.beam.logical.timestamp.millis.rfLocalDateTimeMillis
  implicit val icebergIOITRecordRowType: RowType[IcebergIOITRecord] = RowType[IcebergIOITRecord]
}

class IcebergIOIT extends PipelineSpec with ForAllTestContainer {
  val ContainerPort = 8181
  val CatalogName = "iceberg_it"
  val NamespaceName = "iceberg_it_ns"
  val TableName = s"${NamespaceName}.iceberg_records"

  lazy val tempDir: File = {
    val t = Files.createTempDirectory("iceberg-it").toFile
    t.deleteOnExit()
    t
  }

  override val container: GenericContainer =
    GenericContainer(
      GenericContainer.stringToDockerImage("tabulario/iceberg-rest:1.6.0"),
      exposedPorts = Seq(ContainerPort),
      waitStrategy = new HostPortWaitStrategy()
        .forPorts(ContainerPort)
        .withStartupTimeout(Duration.ofSeconds(180))
    )

  lazy val uri = s"http://${container.containerIpAddress}:${container.mappedPort(ContainerPort)}"

  lazy val tableSchema = new Schema(
    NestedField.required(1, "ts", TimestampType.withZone()),
    NestedField.required(2, "tsWithoutZone", TimestampType.withoutZone()),
    NestedField.required(3, "a", IntegerType.get()),
    NestedField.required(4, "b", StringType.get()),
    NestedField.required(
      5,
      "c",
      StructType.of(NestedField.required(6, "d", BooleanType.get()))
    )
  )

  lazy val catalog: RESTCatalog = {
    val cat = new RESTCatalog()
    cat.initialize(CatalogName, Map("uri" -> uri).asJava)
    cat
  }

  override def afterStart(): Unit = {
    catalog.createNamespace(Namespace.of(NamespaceName))
    catalog.createTable(
      TableIdentifier.parse(TableName),
      tableSchema,
      PartitionSpec.unpartitioned()
    )
  }

  override def beforeStop(): Unit = catalog.close()

  "IcebergIO" should "work" in {
    val catalogProperties = Map(
      CatalogUtil.ICEBERG_CATALOG_TYPE -> CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
      CatalogProperties.URI -> uri
    )
    val ts = Instant.now().truncatedTo(ChronoUnit.MICROS)
    val tsWithoutZone = LocalDateTime.ofInstant(ts, ZoneOffset.UTC)
    val elements =
      1.to(10).map(i => IcebergIOITRecord(ts, tsWithoutZone, i, s"$i", Nested(i % 2 == 0)))

    runWithRealContext() { sc =>
      sc.parallelize(elements)
        .saveAsIceberg(TableName, catalogProperties = catalogProperties)
    }

    runWithRealContext() { sc =>
      sc.iceberg[IcebergIOITRecord](
        TableName,
        catalogProperties = catalogProperties
      ) should containInAnyOrder(elements)
    }
  }

  it should "propagate Iceberg dynamic table creation properties" in {
    val tableName = s"${NamespaceName}.dynamic_table_creation"
    val catalogProperties = Map(
      CatalogUtil.ICEBERG_CATALOG_TYPE -> CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
      CatalogProperties.URI -> uri
    )
    val elements =
      1.to(100).map { i =>
        val ts = Instant.now()
        IcebergIOITRecord(
          ts,
          LocalDateTime.ofInstant(ts, ZoneOffset.UTC),
          i,
          s"value_$i",
          Nested(i % 2 == 0)
        )
      }

    val customWriteDataPath = s"$tempDir/custom_path"

    runWithRealContext() { sc =>
      sc.parallelize(elements)
        .saveAsIceberg(
          tableName,
          catalogProperties = catalogProperties,
          tableProperties = Map(
            "write.data.path" -> customWriteDataPath,
            "write.parquet.bloom-filter-enabled.column.b" -> "true"
          ),
          partitionFields = List("day(ts)"),
          sortFields = List("a asc nulls first")
        )
    }

    val table = catalog.loadTable(TableIdentifier.parse(tableName))
    table.schema().sameSchema(tableSchema) shouldBe true

    // Validate PartitionSpec and SortOrder
    table.spec() shouldEqual PartitionSpec.builderFor(table.schema()).day("ts").build()
    table.sortOrder() shouldEqual SortOrder
      .builderFor(table.schema())
      .asc("a", NullOrder.NULLS_FIRST)
      .build()

    // Validate table properties
    table.properties().get("write.data.path") shouldBe customWriteDataPath

    val tasks = table.newScan().planFiles()
    try {
      val dataFiles = tasks.iterator().asScala.map(_.file().location()).toSeq
      dataFiles should not be empty

      dataFiles.foreach { path =>
        path should startWith(s"$customWriteDataPath/ts_day=")
        val reader = ParquetFileReader.open(BeamInputFile.of(path))
        try {
          reader.getFooter.getBlocks.asScala.foreach { block =>
            block.getColumns.asScala.foreach { col =>
              val hasBloom = col.getBloomFilterOffset > 0
              col.getPath.toDotString match {
                case "b" =>
                  hasBloom shouldBe true
                case _ =>
                  hasBloom shouldBe false
              }
            }
          }
        } finally {
          reader.close()
        }
      }
    } finally {
      tasks.close()
    }
  }
}
