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

package com.spotify.scio.bigtable.syntax

import com.google.bigtable.v2.{Row, RowFilter}
import com.google.cloud.bigtable.admin.v2.models.GCRules.GCRule
import com.spotify.scio.ScioContext
import com.spotify.scio.bigtable.{Admin, BigtableRead}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.joda.time.Duration

object ScioContextOps {
  private val DefaultSleepDuration: Duration = Duration.standardMinutes(20)
  private val DefaultClusterIds: Set[String] = Set.empty
}

/** Enhanced version of [[ScioContext]] with Bigtable methods. */
final class ScioContextOps(private val self: ScioContext) extends AnyVal {
  import ScioContextOps._

  /** Get an SCollection for a Bigtable table. */
  def bigtable(
    projectId: String,
    instanceId: String,
    tableId: String,
    keyRanges: Seq[ByteKeyRange] = BigtableRead.ReadParam.DefaultKeyRanges,
    rowFilter: RowFilter = BigtableRead.ReadParam.DefaultRowFilter,
    maxBufferElementCount: Option[Int] = BigtableRead.ReadParam.DefaultMaxBufferElementCount
  ): SCollection[Row] = {
    val parameters = BigtableRead.ReadParam(keyRanges, rowFilter, maxBufferElementCount)
    self.read(BigtableRead(projectId, instanceId, tableId))(parameters)
  }

  /**
   * Updates given clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at the
   * end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param numberOfNodes
   *   desired number of nodes for the clusters
   */
  def updateNumberOfBigtableNodes(
    projectId: String,
    instanceId: String,
    numberOfNodes: Int
  ): Unit =
    updateNumberOfBigtableNodes(
      projectId,
      instanceId,
      DefaultClusterIds,
      numberOfNodes,
      DefaultSleepDuration
    )

  /**
   * Updates given clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at the
   * end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param numberOfNodes
   *   desired number of nodes for the clusters
   * @param sleepDuration
   *   How long to sleep after updating the number of nodes. Google recommends at least 20 minutes
   *   before the new nodes are fully functional
   */
  def updateNumberOfBigtableNodes(
    projectId: String,
    instanceId: String,
    numberOfNodes: Int,
    sleepDuration: Duration
  ): Unit =
    updateNumberOfBigtableNodes(
      projectId,
      instanceId,
      DefaultClusterIds,
      numberOfNodes,
      sleepDuration
    )

  /**
   * Updates given clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at the
   * end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param numberOfNodes
   *   desired number of nodes for the clusters
   * @param clusterIds
   *   clusters ids to be updated, all if empty
   * @param sleepDuration
   *   How long to sleep after updating the number of nodes. Google recommends at least 20 minutes
   *   before the new nodes are fully functional
   */
  def updateNumberOfBigtableNodes(
    projectId: String,
    instanceId: String,
    clusterIds: Set[String],
    numberOfNodes: Int,
    sleepDuration: Duration = DefaultSleepDuration
  ): Unit =
    if (!self.isTest) {
      // No need to update the number of nodes in a test
      Admin.Instance.updateNumberOfBigtableNodes(
        projectId,
        instanceId,
        clusterIds,
        numberOfNodes,
        sleepDuration
      )
    }

  /**
   * Get size of all clusters for specified Bigtable instance.
   *
   * @return
   *   map of clusterId to its number of nodes
   */
  def getBigtableClusterSizes(projectId: String, instanceId: String): Map[String, Int] =
    if (!self.isTest) {
      Admin.Instance.getClusterSizes(projectId, instanceId)
    } else {
      Map.empty
    }

  /**
   * Ensure that tables and column families exist. Checks for existence of tables or creates them if
   * they do not exist. Also checks for existence of column families within each table and creates
   * them if they do not exist.
   *
   * @param tablesAndColumnFamilies
   *   A map of tables and column families. Keys are table names. Values are a list of column family
   *   names.
   */
  def ensureTable(
    projectId: String,
    instanceId: String,
    tableId: String,
    columnFamilies: Iterable[String],
    createDisposition: Admin.Table.CreateDisposition = Admin.Table.CreateDisposition.Default
  ): Unit =
    if (!self.isTest) {
      Admin.Table.ensureTable(projectId, instanceId, tableId, columnFamilies, createDisposition)
    }

  /**
   * Ensure that tables and column families exist. Checks for existence of tables or creates them if
   * they do not exist. Also checks for existence of column families within each table and creates
   * them if they do not exist.
   *
   * @param columnFamiliesWithExpiration
   *   A map of tables and column families. Keys are table names. Values are a list of column family
   *   names along with the desired cell expiration. Cell expiration is the duration before which
   *   garbage collection of a cell may occur. Note: minimum granularity is second.
   */
  def ensureTablesWithExpiration(
    projectId: String,
    instanceId: String,
    tableId: String,
    columnFamiliesWithExpiration: Iterable[(String, Option[Duration])],
    createDisposition: Admin.Table.CreateDisposition = Admin.Table.CreateDisposition.Default
  ): Unit =
    if (!self.isTest) {
      Admin.Table.ensureTablesWithExpiration(
        projectId,
        instanceId,
        tableId,
        columnFamiliesWithExpiration,
        createDisposition
      )
    }

  /**
   * Ensure that tables and column families exist. Checks for existence of tables or creates them if
   * they do not exist. Also checks for existence of column families within each table and creates
   * them if they do not exist.
   *
   * @param tablesAndColumnFamiliesWithGcRules
   *   A map of tables and column families. Keys are table names. Values are a list of column family
   *   names along with the desired GcRule.
   */
  def ensureTablesWithGcRules(
    projectId: String,
    instanceId: String,
    tableId: String,
    columnFamiliesWithGcRules: Iterable[(String, Option[GCRule])],
    createDisposition: Admin.Table.CreateDisposition
  ): Unit =
    if (!self.isTest) {
      Admin.Table.ensureTableWithGcRules(
        projectId,
        instanceId,
        tableId,
        columnFamiliesWithGcRules,
        createDisposition
      )
    }
}

trait ScioContextSyntax {
  implicit def bigtableScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
