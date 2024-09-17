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
import com.spotify.scio.testing.PipelineSpec
import magnolify.beam._
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.types.Types.{IntegerType, NestedField, StringType}
import org.apache.iceberg.{CatalogProperties, CatalogUtil, PartitionSpec}
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import java.time.Duration
import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters._

case class IcebergIOITRecord(a: Int, b: String)
object IcebergIOITRecord {
  implicit val icebergIOITRecordRowType: RowType[IcebergIOITRecord] = RowType[IcebergIOITRecord]
}

class IcebergIOIT extends PipelineSpec with ForAllTestContainer { // with BeforeAndAfterAll with Eventually  { //  {
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

  override def afterStart(): Unit = {
    val cat = new RESTCatalog()
    cat.initialize(CatalogName, Map("uri" -> uri).asJava)

    import org.apache.iceberg.Schema
    cat.createNamespace(Namespace.of(NamespaceName))
    cat.createTable(
      TableIdentifier.parse(TableName),
      new Schema(
        NestedField.required(0, "a", IntegerType.get()),
        NestedField.required(1, "b", StringType.get())
      ),
      PartitionSpec.unpartitioned()
    )
  }

  "IcebergIO" should "work" in {
    val catalogProperties = Map(
      CatalogUtil.ICEBERG_CATALOG_TYPE -> CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
      CatalogProperties.URI -> uri
    )
    val elements = 1.to(10).map(i => IcebergIOITRecord(i, s"$i"))

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
}
