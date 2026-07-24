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
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.types.Types.{
  BooleanType,
  IntegerType,
  NestedField,
  StringType,
  StructType
}
import org.apache.iceberg.{
  CatalogProperties,
  CatalogUtil,
  NullOrder,
  PartitionSpec,
  Schema,
  SortDirection
}
import org.apache.parquet.hadoop.ParquetFileReader
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import java.time.Duration
import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters._

case class Nested(d: Boolean)
case class IcebergIOITRecord(a: Int, b: String, c: Nested)
object IcebergIOITRecord {
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

  lazy val catalog: RESTCatalog = {
    val cat = new RESTCatalog()
    cat.initialize(CatalogName, Map("uri" -> uri).asJava)
    cat
  }

  override def afterStart(): Unit = {
    catalog.createNamespace(Namespace.of(NamespaceName))
    catalog.createTable(
      TableIdentifier.parse(TableName),
      new Schema(
        NestedField.required(0, "a", IntegerType.get()),
        NestedField.required(1, "b", StringType.get()),
        NestedField.required(
          2,
          "c",
          StructType.of(NestedField.required(3, "d", BooleanType.get()))
        )
      ),
      PartitionSpec.unpartitioned()
    )
  }

  "IcebergIO" should "work" in {
    val catalogProperties = Map(
      CatalogUtil.ICEBERG_CATALOG_TYPE -> CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
      CatalogProperties.URI -> uri
    )
    val elements = 1.to(10).map(i => IcebergIOITRecord(i, s"$i", Nested(i % 2 == 0)))

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

  it should "write with sort order" in {
    val sortedTableName = s"${NamespaceName}.sorted_records"
    val catalogProperties = Map(
      CatalogUtil.ICEBERG_CATALOG_TYPE -> CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
      CatalogProperties.URI -> uri
    )
    val elements = 1.to(10).map(i => IcebergIOITRecord(i, s"$i", Nested(i % 2 == 0)))

    runWithRealContext() { sc =>
      sc.parallelize(elements)
        .saveAsIceberg(
          sortedTableName,
          catalogProperties = catalogProperties,
          sortFields = List("a asc nulls first")
        )
    }

    val table = catalog.loadTable(TableIdentifier.parse(sortedTableName))
    val sortOrder = table.sortOrder()
    sortOrder.isSorted shouldBe true
    sortOrder.fields().size() shouldBe 1
    sortOrder.fields().get(0).direction() shouldBe SortDirection.ASC
    sortOrder.fields().get(0).nullOrder() shouldBe NullOrder.NULLS_FIRST
  }

  it should "write with partition spec" in {
    val partitionedTableName = s"${NamespaceName}.partitioned_records"
    val catalogProperties = Map(
      CatalogUtil.ICEBERG_CATALOG_TYPE -> CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
      CatalogProperties.URI -> uri
    )
    val elements = 1.to(10).map(i => IcebergIOITRecord(i, s"$i", Nested(i % 2 == 0)))

    runWithRealContext() { sc =>
      sc.parallelize(elements)
        .saveAsIceberg(
          partitionedTableName,
          catalogProperties = catalogProperties,
          partitionFields = List("bucket(b, 2)")
        )
    }

    val table = catalog.loadTable(TableIdentifier.parse(partitionedTableName))
    val spec = table.spec()
    spec.isPartitioned shouldBe true
    spec.fields().size() shouldBe 1
    spec.fields().get(0).name() shouldBe "b_bucket"
  }

  it should "propagate Iceberg writeProperties" in {
    val bfTableName = s"${NamespaceName}.bloom_filter_records"
    val catalogProperties = Map(
      CatalogUtil.ICEBERG_CATALOG_TYPE -> CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
      CatalogProperties.URI -> uri
    )
    val elements = 1.to(100).map(i => IcebergIOITRecord(i, s"value_$i", Nested(i % 2 == 0)))

    runWithRealContext() { sc =>
      sc.parallelize(elements)
        .saveAsIceberg(
          bfTableName,
          catalogProperties = catalogProperties,
          writeProperties = Map("write.parquet.bloom-filter-enabled.column.b" -> "true")
        )
    }

    val table = catalog.loadTable(TableIdentifier.parse(bfTableName))
    val tasks = table.newScan().planFiles()
    try {
      val dataFiles = tasks.iterator().asScala.map(_.file().path().toString).toSeq
      dataFiles should not be empty

      dataFiles.foreach { path =>
        val reader = ParquetFileReader.open(BeamInputFile.of(path))
        try {
          reader.getFooter.getBlocks.asScala.foreach { block =>
            block.getColumns.asScala.foreach { col =>
              val hasBloom = col.getBloomFilterOffset > 0
              col.getPath.toDotString match {
                case "b" =>
                  // flip assertion once https://github.com/apache/beam/pull/39250/ is release in Beam 2.76
                  withClue(
                    "Iceberg writeProperties are not supported in Beam versions <= 2.76. Once Beam is upgraded, flip this assertion to `true`."
                  ) {
                    hasBloom shouldBe false
                  }
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
