/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.bigtable

import com.google.cloud.bigtable.beam.CloudBigtableConfiguration
import com.google.cloud.bigtable.hbase.BigtableConfiguration
import com.spotify.scio._
import com.spotify.scio.testing._
import org.apache.hadoop.hbase.client.{Delete, Put, Result}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.joda.time.Duration

import java.nio.charset.StandardCharsets
import java.util.UUID

object BigtableIT {
  val ProjectId = "data-integration-test"
  val InstanceId = "scio-bigtable-it"
  val ClusterId = "scio-bigtable-it-cluster"
  val TableId = "scio-bigtable-it-counts"
  val ZoneId = "us-central1-f"

  val Config: CloudBigtableConfiguration = new CloudBigtableConfiguration.Builder()
    .withProjectId(ProjectId)
    .withInstanceId(InstanceId)
    .build()

  val TableFamily = new HColumnDescriptor("count")
  val TableColumnQualifier: Array[Byte] = "long".getBytes(StandardCharsets.UTF_8)
  val Table: HTableDescriptor = new HTableDescriptor(TableName.valueOf(TableId))
    .addFamily(TableFamily)

  val TestId: String = UUID.randomUUID().toString.substring(0, 8)
  val TestData: Seq[(String, Long)] = Seq(
    s"$TestId-key1" -> 1L,
    s"$TestId-key2" -> 2L,
    s"$TestId-key3" -> 3L
  )
  def toPutMutation(key: String, value: Long): Put =
    new Put(key.getBytes(StandardCharsets.UTF_8))
      .addColumn(TableFamily.getName, TableColumnQualifier, BigInt(value).toByteArray)

  def toDeleteMutation(key: String): Delete =
    new Delete(key.getBytes(StandardCharsets.UTF_8))
      .addColumns(TableFamily.getName, TableColumnQualifier)

  def fromResult(r: Result): (String, Long) = {
    val key = new String(r.getRow, StandardCharsets.UTF_8)
    val value = BigInt(r.getValue(TableFamily.getName, TableColumnQualifier)).toLong
    key -> value
  }
}

class BigtableIT extends PipelineSpec {
  import BigtableIT._

  "BigtableIO" should "work" in {
    TableAdmin.ensureTables(Config, Seq(Table))
    try {
      // Write rows to table
      val sc1 = ScioContext()
      sc1
        .parallelize(TestData)
        .map { case (key, value) => toPutMutation(key, value) }
        .saveAsBigtable(ProjectId, InstanceId, TableId)
      sc1.run().waitUntilFinish()

      // Read rows back
      val sc2 = ScioContext()
      // Filter rows in case there are other keys in the table
      val filter = new PrefixFilter(TestId.getBytes(StandardCharsets.UTF_8))
      sc2
        .bigtable(ProjectId, InstanceId, TableId, filter = filter)
        .map(fromResult) should containInAnyOrder(TestData)
      sc2.run().waitUntilFinish()
    } finally {
      // Delete rows afterwards
      val sc = ScioContext()
      sc.parallelize(TestData)
        .keys
        .map(toDeleteMutation)
        .saveAsBigtable(ProjectId, InstanceId, TableId)
      sc.run().waitUntilFinish()
    }
  }

  behavior of "InstanceAdmin"
  ignore should "work" in {
    InstanceAdmin.resizeClusters(Config, 3, Duration.standardSeconds(10))
    InstanceAdmin.getCluster(Config, ClusterId) shouldBe 2

    InstanceAdmin.resizeClusters(Config, Set(ClusterId), 1, Duration.standardSeconds(10))
    InstanceAdmin.getCluster(Config, ClusterId) shouldBe 1
  }

  "TableAdmin" should "work" in {
    val tables = Seq(
      new HTableDescriptor(TableName.valueOf(s"scio-bigtable-empty-table-$TestId")),
      new HTableDescriptor(TableName.valueOf(s"scio-bigtable-one-cf-table-$TestId"))
        .addFamily(new HColumnDescriptor("colfam1")),
      new HTableDescriptor(TableName.valueOf(s"scio-bigtable-two-cf-table-$TestId"))
        .addFamily(new HColumnDescriptor("colfam1"))
        .addFamily(new HColumnDescriptor("colfam2"))
    )

    val connection = BigtableConfiguration.connect(Config.toHBaseConfig)
    val client = connection.getAdmin
    try {
      // Delete any tables that could be left around from previous IT run.
      client
        .listTableNames(s".*$TestId".r.pattern)
        .foreach(client.deleteTable)

      // Ensure that the tables don't exist now
      client.listTableNames(s".*$TestId".r.pattern) shouldBe empty

      // Run UT
      TableAdmin.ensureTables(Config, tables)

      // Tables must exist with exact settings
      val actualTables = client.listTables(s".*$TestId".r.pattern)
      actualTables should contain theSameElementsAs tables
    } finally {
      tables.foreach(t => client.deleteTable(t.getTableName))
      connection.close()
    }
  }
}
