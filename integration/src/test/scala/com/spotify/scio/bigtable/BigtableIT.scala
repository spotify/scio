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

import java.util.UUID
import com.google.bigtable.v2.{Mutation, Row, RowFilter}
import com.google.cloud.bigtable.admin.v2.{BigtableInstanceAdminClient, BigtableTableAdminClient}
import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.testing._
import org.joda.time.Duration

import scala.jdk.CollectionConverters._

object BigtableIT {
  val projectId = "data-integration-test"
  val instanceId = "scio-bigtable-it"
  val clusterId = "scio-bigtable-it-cluster"
  val zoneId = "us-central1-f"
  val tableId = "scio-bigtable-it-counts"

  def testId(): String = UUID.randomUUID().toString.substring(0, 8)
  def testData(id: String): Seq[(String, Long)] =
    Seq((s"$id-key1", 1L), (s"$id-key2", 2L), (s"$id-key3", 3L))

  val FAMILY_NAME: String = "count"
  val COLUMN_QUALIFIER: ByteString = ByteString.copyFromUtf8("long")

  def toWriteMutation(key: String, value: Long): (ByteString, Iterable[Mutation]) = {
    val m = Mutations.newSetCell(
      FAMILY_NAME,
      COLUMN_QUALIFIER,
      ByteString.copyFromUtf8(value.toString),
      0L
    )
    (ByteString.copyFromUtf8(key), Iterable(m))
  }

  def toDeleteMutation(key: String): (ByteString, Iterable[Mutation]) = {
    val m = Mutations.newDeleteFromRow
    (ByteString.copyFromUtf8(key), Iterable(m))
  }

  def fromRow(r: Row): (String, Long) =
    (r.getKey.toStringUtf8, r.getValue(FAMILY_NAME, COLUMN_QUALIFIER).get.toStringUtf8.toLong)
}

class BigtableIT extends PipelineSpec {
  import BigtableIT._

  // "Update number of bigtable nodes" should "work" in {
  ignore should "update number of bigtable nodes" in {
    val client = BigtableInstanceAdminClient.create(projectId)
    try {
      val sc = ScioContext()
      sc.updateNumberOfBigtableNodes(projectId, instanceId, 4, Duration.standardSeconds(10))
      sc.getBigtableClusterSizes(projectId, instanceId)(clusterId) shouldBe 4
      client.getCluster(clusterId, zoneId).getServeNodes shouldBe 4
      sc.updateNumberOfBigtableNodes(projectId, instanceId, 3, Duration.standardSeconds(10))
      sc.getBigtableClusterSizes(projectId, instanceId)(clusterId) shouldBe 3
      client.getCluster(clusterId, zoneId).getServeNodes shouldBe 3
    } finally {
      client.close()
    }
  }

  "BigtableIO" should "work in default mode" in {
    Admin.Table.ensureTable(projectId, instanceId, tableId, List(FAMILY_NAME))
    val id = testId()
    val data = testData(id)
    try {
      // Write rows to table
      runWithRealContext() { sc =>
        sc
          .parallelize(data.map(kv => toWriteMutation(kv._1, kv._2)))
          .saveAsBigtable(projectId, instanceId, tableId)
      }.waitUntilDone()

      // Read rows back
      // Filter rows in case there are other keys in the table
      val rowFilter = RowFilter
        .newBuilder()
        .setRowKeyRegexFilter(ByteString.copyFromUtf8(s"$id-.*"))
        .build()
      runWithRealContext() { sc =>
        sc
          .bigtable(projectId, instanceId, tableId, rowFilter = rowFilter)
          .map(fromRow) should containInAnyOrder(data)
      }.waitUntilDone()
    } catch {
      case e: Throwable => throw e
    } finally
      {
        // Delete rows afterwards
        runWithRealContext() { sc =>
          sc.parallelize(data.map(kv => toDeleteMutation(kv._1)))
            .saveAsBigtable(projectId, instanceId, tableId)
        }
      }.waitUntilFinish()
  }

  it should "work in bulk mode" in {
    Admin.Table.ensureTable(projectId, instanceId, tableId, List(FAMILY_NAME))
    val id = testId()
    val data = testData(id)

    try {
      // Write rows to table with batch
      runWithRealContext() { sc =>
        sc
          .parallelize(data.map(kv => toWriteMutation(kv._1, kv._2)))
          .saveAsBigtable(projectId, instanceId, tableId)
      }.waitUntilDone()

      // Read rows back
      // Filter rows in case there are other keys in the table
      val rowFilter = RowFilter
        .newBuilder()
        .setRowKeyRegexFilter(ByteString.copyFromUtf8(s"$id-.*"))
        .build()
      runWithRealContext() { sc =>
        sc
          .bigtable(projectId, instanceId, tableId, rowFilter = rowFilter)
          .map(fromRow) should containInAnyOrder(data)
      }.waitUntilDone()
    } catch {
      case e: Throwable => throw e
    } finally
      {
        // Delete rows afterwards
        runWithRealContext() { sc =>
          sc.parallelize(data.map(kv => toDeleteMutation(kv._1)))
            .saveAsBigtable(projectId, instanceId, tableId)
        }
      }.waitUntilFinish()
  }

  "Admin.Table" should "work" in {
    val id = testId()
    val tables = Map(
      s"scio-bigtable-empty-table-$id" -> List(),
      s"scio-bigtable-one-cf-table-$id" -> List("colfam1"),
      s"scio-bigtable-two-cf-table-$id" -> List("colfam1", "colfam2")
    )

    val client = BigtableTableAdminClient.create(projectId, instanceId)
    try {
      val tableIds = tables.keys.toSet

      // Delete any tables that could be left around from previous IT run.
      client
        .listTables()
        .asScala
        .filterNot(tableIds.contains)
        .foreach(client.deleteTable)

      // Ensure that the tables don't exist now
      client.listTables().asScala.toSet.intersect(tableIds) shouldBe empty

      // Run UUT
      tables.foreach { case (tableId, cfs) =>
        Admin.Table.ensureTable(projectId, instanceId, tableId, cfs)
      }

      // Tables must exist
      client.listTables().asScala should contain allElementsOf tableIds

      // Assert Column families exist
      tables.foreach { case (id, columnFamilies) =>
        val table = client.getTable(id)
        val actualFamilies = table.getColumnFamilies.asScala.map(_.getId)

        actualFamilies should contain theSameElementsAs columnFamilies
      }

      // Clean up and delete
      tableIds.foreach(client.deleteTable)
    } finally {
      client.close()
    }
  }
}
