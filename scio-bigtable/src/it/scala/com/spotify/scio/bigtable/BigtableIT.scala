/*
 * Copyright 2017 Spotify AB.
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

import java.util.UUID

import com.google.bigtable.admin.v2.{DeleteTableRequest, GetTableRequest, ListTablesRequest}
import com.google.bigtable.v2.{Mutation, Row, RowFilter}
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc._
import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.testing._
import org.joda.time.Duration

import scala.collection.JavaConverters._

object BigtableIT {

  val projectId = "data-integration-test"
  val instanceId = "scio-bigtable-it"
  val clusterId = "scio-bigtable-it-cluster"
  val zoneId = "us-east1-b"
  val tableId = "scio-bigtable-it-counts"
  val uuid = UUID.randomUUID().toString.substring(0, 8)
  val testData = Seq((s"$uuid-key1", 1L), (s"$uuid-key2", 2L), (s"$uuid-key3", 3L))

  val bigtableOptions = new BigtableOptions.Builder()
    .setProjectId(projectId)
    .setInstanceId(instanceId)
    .build

  val FAMILY_NAME: String = "count"
  val COLUMN_QUALIFIER: ByteString = ByteString.copyFromUtf8("long")

  def toWriteMutation(key: String, value: Long): (ByteString, Iterable[Mutation]) = {
    val m = Mutations.newSetCell(
      FAMILY_NAME, COLUMN_QUALIFIER, ByteString.copyFromUtf8(value.toString), 0L)
    (ByteString.copyFromUtf8(key), Iterable(m))
  }

  def toDeleteMutation(key: String): (ByteString, Iterable[Mutation]) = {
    val m = Mutations.newDeleteFromRow
    (ByteString.copyFromUtf8(key), Iterable(m))
  }

  def fromRow(r: Row): (String, Long) =
    (r.getKey.toStringUtf8, r.getValue(FAMILY_NAME, COLUMN_QUALIFIER).get.toStringUtf8.toLong)

  def listTables(client: BigtableTableAdminGrpcClient): Set[String] = {
    val instancePath = s"projects/$projectId/instances/$instanceId"
    val tables = client.listTables(ListTablesRequest.newBuilder().setParent(instancePath).build)
    tables.getTablesList.asScala.map(t =>
      new BigtableTableName(t.getName).getTableId)
      .toSet
  }
}

class BigtableIT extends PipelineSpec {

  import BigtableIT._

  // "Update number of bigtable nodes" should "work" in {
  ignore should "update number of bigtable nodes" in {
    val bt = new BigtableClusterUtilities(bigtableOptions)
    val sc = ScioContext()
    sc.updateNumberOfBigtableNodes(projectId, instanceId, 4, Duration.standardSeconds(10))
    sc.getBigtableClusterSizes(projectId, instanceId)(clusterId)  shouldBe 4
    bt.getClusterNodeCount(clusterId, zoneId) shouldBe 4
    sc.updateNumberOfBigtableNodes(projectId, instanceId, 3, Duration.standardSeconds(10))
    sc.getBigtableClusterSizes(projectId, instanceId)(clusterId) shouldBe 3
    bt.getClusterNodeCount(clusterId, zoneId) shouldBe 3
  }

  "BigtableIO" should "work" in {
    TableAdmin.ensureTables(bigtableOptions, Map(tableId -> List(FAMILY_NAME)))
    try {
      // Write rows to table
      val sc1 = ScioContext()
      sc1
        .parallelize(testData.map(kv => toWriteMutation(kv._1, kv._2)))
        .saveAsBigtable(projectId, instanceId, tableId)
      sc1.close().waitUntilFinish()

      // Read rows back
      val sc2 = ScioContext()
      // Filter rows in case there are other keys in the table
      val rowFilter = RowFilter.newBuilder()
        .setRowKeyRegexFilter(ByteString.copyFromUtf8(s"$uuid-.*"))
        .build()
      sc2
        .bigtable(projectId, instanceId, tableId, rowFilter = rowFilter)
        .map(fromRow) should containInAnyOrder (testData)
      sc2.close().waitUntilFinish()
    } catch {
      case e: Throwable => throw e
    } finally {
      // Delete rows afterwards
      val sc = ScioContext()
      sc.parallelize(testData.map(kv => toDeleteMutation(kv._1)))
        .saveAsBigtable(projectId, instanceId, tableId)
      sc.close().waitUntilFinish()
    }
  }

  "TableAdmin" should "work" in {
    val tables = Map(
      s"scio-bigtable-empty-table-$uuid" -> List(),
      s"scio-bigtable-one-cf-table-$uuid" -> List("colfam1"),
      s"scio-bigtable-two-cf-table-$uuid" -> List("colfam1", "colfam2")
    )
    val channel = ChannelPoolCreator.createPool(bigtableOptions.getAdminHost)
    val executorService = BigtableSessionSharedThreadPools.getInstance().getRetryExecutor
    val client = new BigtableTableAdminGrpcClient(channel, executorService, bigtableOptions)
    val instancePath = s"projects/$projectId/instances/$instanceId"
    val tableIds = tables.keys.toSet
    def tablePath(table: String): String = s"$instancePath/tables/$table"
    def deleteTable(table: String): Unit =
      client.deleteTable(DeleteTableRequest.newBuilder().setName(tablePath(table)).build)

    // Delete any tables that could be left around from previous IT run.
    val oldTables = listTables(client).intersect(tableIds)
    oldTables.foreach(deleteTable)

    // Ensure that the tables don't exist now
    listTables(client).intersect(tableIds) shouldBe empty

    // Run UUT
    TableAdmin.ensureTables(bigtableOptions, tables)

    // Tables must exist
    listTables(client).intersect(tableIds) shouldEqual tableIds

    // Assert Column families exist
    for ((table, columnFamilies) <- tables) {
      val tableInfo = client.getTable(GetTableRequest.newBuilder()
        .setName(tablePath(table)).build)
      val actualColumnFamilies = tableInfo.getColumnFamiliesMap.asScala.keys
      actualColumnFamilies should contain theSameElementsAs columnFamilies
    }

    // Clean up and delete
    tables.keys.foreach(deleteTable)
  }

}
