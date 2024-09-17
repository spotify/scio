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

//import com.dimafeng.testcontainers.GenericContainer.FileSystemBind
import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import org.apache.iceberg.{CatalogProperties, CatalogUtil}
//import com.google.common.collect.ImmutableMap
import com.spotify.scio.testing.PipelineSpec

import java.sql.DriverManager
//import org.scalatest.concurrent.Eventually
import magnolify.beam._
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
//import org.apache.hadoop.hive.metastore.api.{Catalog, Database, Table}
//import org.apache.iceberg.PartitionSpec
//import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
//import org.apache.iceberg.hive.HiveCatalog
//import org.apache.iceberg.types.Types.{IntegerType, NestedField, StringType}

import java.time.Duration
//import org.scalatest.BeforeAndAfterAll
//import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy

import java.io.File
import java.nio.file.Files

case class IcebergIOITRecord(a: Int, b: String, i: Int)
object IcebergIOITRecord {
  implicit val icebergIOITRecordRowType: RowType[IcebergIOITRecord] = RowType[IcebergIOITRecord]
}

class IcebergIOIT extends PipelineSpec  with ForAllTestContainer { //with BeforeAndAfterAll with Eventually  { //  {
  val warehouseLocalPath: String = "/opt/hive/data/warehouse/iceberg-it/"
  lazy val tempDir: File = {
    val t = Files.createTempDirectory("iceberg-it").toFile
    t.deleteOnExit()
    t
  }

  // docker run -d -p 9083:9083 -v /Users/kellend/tmp/iceberg-it:/opt/hive/data/warehouse/iceberg-it/ --env SERVICE_NAME=metastore apache/hive:3.1.3
  // docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --env VERBOSE=true apache/hive:3.1.3
  override val container: GenericContainer =
    GenericContainer(
      GenericContainer.stringToDockerImage("apache/hive:4.0.0"),
      env = Map(
        "SERVICE_NAME" -> "hiveserver2",
        "VERBOSE" -> "true",
        "HIVE_SERVER2_THRIFT_PORT" -> "10000",
        "SERVICE_OPTS" -> "-Dhive.metastore.uris=thrift://0.0.0.0:9083'"
      ),
      exposedPorts = Seq(10000, 10002),
//      fileSystemBind = Seq(
//        FileSystemBind(s"$tempDir", "/warehouse", BindMode.READ_WRITE)
//      ),
      waitStrategy = new HostPortWaitStrategy()
        .forPorts(10000, 10002)
        // hive metastore
        .withStartupTimeout(Duration.ofSeconds(180))
    )

      override def afterStart(): Unit = {
//    val conf = new Configuration(false)
//    configMap.foreach { case (k, v) => conf.set(k, v) }
//    val client = new HiveMetaStoreClient(conf)
//
//    client.createCatalog(
//      new Catalog(
//        "iceberg_it",
//        s"file://${warehouseLocalPath}"
//      )
//    )
//
//    client.createDatabase(
//      new Database(
//        "iceberg_it",
//        "iceberg test records",
//        s"file://${warehouseLocalPath}/iceberg_it.db/",
//        ImmutableMap.of[String, String]()
//      )
//    )
//
//    client.createTable(
//      new Table(
//        "iceberg_records",
//        "iceberg_it",
//        "iceberg_owner",
//        0,
//        0,
//        Int.MaxValue,
//        /*
//          private String tableName; // required
//  private String dbName; // required
//  private String owner; // required
//  private int createTime; // required
//  private int lastAccessTime; // required
//  private int retention; // required
//
//  private StorageDescriptor sd; // required
//  private List<FieldSchema> partitionKeys; // required
//  private Map<String,String> parameters; // required
//  private String viewOriginalText; // required
//  private String viewExpandedText; // required
//  private String tableType; // required
//
//  private PrincipalPrivilegeSet privileges; // optional
//  private boolean temporary; // optional
//  private boolean rewriteEnabled; // optional
//  private CreationMetadata creationMetadata; // optional
//  private String catName; // optional
//  private PrincipalType ownerType; // optional
//         */
//
//      )
//    )

//    // Note: this operates directly on the tempdir
//    new File(tempDir + File.separator + "iceberg_it.db" + "iceberg_records").mkdirs()
//    val cat = new HiveCatalog()
//    cat.initialize(
//      "iceberg_it",
//      ImmutableMap.of[String, String](
//        "type", "hive",
//        "uri", s"thrift://${container.containerIpAddress}:${container.mappedPort(9083)}",
////        "uri", s"thrift://0.0.0.0:9083",
//        "warehouse", s"file://${tempDir}"
//      )
//    )
//
//    import org.apache.iceberg.Schema
//    cat.dropNamespace(Namespace.of("iceberg_it"))
//    cat.createNamespace(Namespace.of("iceberg_it"))
//    cat.createTable(
//      TableIdentifier.parse("iceberg_it.iceberg_records"),
//      new Schema(
//        NestedField.required(0, "a", IntegerType.get()),
//        NestedField.required(1, "b", StringType.get()),
//      ),
//      PartitionSpec.unpartitioned()
//    )

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conStr = s"jdbc:hive2://${container.containerIpAddress}:${container.mappedPort(10000)}/default"
    val con = DriverManager.getConnection(conStr, "", "")
    val stmt = con.createStatement
    stmt.execute("CREATE DATABASE iceberg_it_db")
//    stmt.execute("CREATE NAMESPACE iceberg_it_ns")
    stmt.execute("CREATE TABLE iceberg_it_db.iceberg_records(a INT, b STRING) PARTITIONED BY (i int) STORED BY ICEBERG")
    stmt.execute("INSERT INTO iceberg_it_db.iceberg_records VALUES(1, '1', 1)")
    con.close()
  }

  "IcebergIO" should "foo" in {
    runWithRealContext() { sc =>
      val elements = 1.to(10).map(i => IcebergIOITRecord(i, s"$i", i))
      sc.parallelize(elements)
        .saveAsIceberg(
          "iceberg_it_db.iceberg_records",
          "iceberg_it_db",
          catalogProperties = Map(
            CatalogUtil.ICEBERG_CATALOG_TYPE -> CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
            CatalogProperties.URI -> s"thrift://${container.containerIpAddress}:${container.mappedPort(10000)}",
//            "warehouse" -> s"file://${warehouseLocalPath}"
          ),
          configProperties = Map(
//            "hive.metastore.sasl.enabled" -> "true",
            "hive.security.authorization.enabled" -> "true",
            "hive.metastore.username" -> "",
          )
        )
    }
  }
}

