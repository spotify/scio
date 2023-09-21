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

import com.google.cloud.bigtable.beam.{CloudBigtableConfiguration, CloudBigtableTableConfiguration}
import com.spotify.scio.ScioContext
import com.spotify.scio.bigtable.{BigtableRead, InstanceAdmin, TableAdmin}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.filter.Filter
import org.joda.time.Duration

object ScioContextOps {
  private val DefaultSleepDuration = Duration.standardMinutes(20)
  Set.empty
}

/** Enhanced version of [[ScioContext]] with Bigtable methods. */
final class ScioContextOps(private val self: ScioContext) extends AnyVal {
  import ScioContextOps._

  /** Get an SCollection for a Bigtable table. */
  def bigtable(
    projectId: String,
    instanceId: String,
    tableId: String,
    keyRange: ByteKeyRange = BigtableRead.ReadParam.DefaultKeyRange,
    filter: Filter = BigtableRead.ReadParam.DefaultFilter
  ): SCollection[Result] = {
    val params = BigtableRead.ReadParam(keyRange, filter)
    self.read(BigtableRead(projectId, instanceId, tableId))(params)
  }

  /** Get an SCollection for a Bigtable table. */
  def bigtable(
    config: CloudBigtableTableConfiguration
  ): SCollection[Result] = {
    val parameters = BigtableRead.ReadParam()
    self.read(BigtableRead(config))(parameters)
  }

  /** Get an SCollection for a Bigtable table. */
  def bigtable(
    config: CloudBigtableTableConfiguration,
    keyRange: ByteKeyRange
  ): SCollection[Result] = {
    val parameters = BigtableRead.ReadParam(keyRange)
    self.read(BigtableRead(config))(parameters)
  }

  /** Get an SCollection for a Bigtable table. */
  def bigtable(
    config: CloudBigtableTableConfiguration,
    keyRange: ByteKeyRange,
    filter: Filter
  ): SCollection[Result] = {
    val parameters = BigtableRead.ReadParam(keyRange, filter)
    self.read(BigtableRead(config))(parameters)
  }

  /**
   * Updates all clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at the
   * end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param sleepDuration
   *   How long to sleep after updating the number of nodes. Google recommends at least 20 minutes
   *   before the new nodes are fully functional
   */
  def resizeClusters(
    projectId: String,
    instanceId: String,
    numServeNodes: Int,
    sleepDuration: Duration = DefaultSleepDuration
  ): Unit = {
    val config = new CloudBigtableConfiguration.Builder()
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .build()
    resizeClusters(config, numServeNodes, sleepDuration)
  }

  /**
   * Updates all clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at the
   * end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param sleepDuration
   *   How long to sleep after updating the number of nodes. Google recommends at least 20 minutes
   *   before the new nodes are fully functional
   */
  def resizeClusters(
    config: CloudBigtableConfiguration,
    numServeNodes: Int,
    sleepDuration: Duration
  ): Unit = if (!self.isTest) {
    InstanceAdmin.resizeClusters(
      config,
      numServeNodes,
      sleepDuration
    )
  }

  /**
   * Updates all clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at the
   * end to lower costs yet still get high throughput during bulk ingests/dumps.
   */
  def resizeClusters(
    projectId: String,
    instanceId: String,
    clusterIds: Set[String],
    numServeNodes: Int
  ): Unit = {
    val config = new CloudBigtableConfiguration.Builder()
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .build()
    resizeClusters(config, clusterIds, numServeNodes, DefaultSleepDuration)
  }

  /**
   * Updates given clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at the
   * end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param clusterNames
   *   Names of clusters to be updated, all if empty
   * @param sleepDuration
   *   How long to sleep after updating the number of nodes. Google recommends at least 20 minutes
   *   before the new nodes are fully functional
   */
  def resizeClusters(
    projectId: String,
    instanceId: String,
    clusterIds: Set[String],
    numServeNodes: Int,
    sleepDuration: Duration
  ): Unit = {
    val config = new CloudBigtableConfiguration.Builder()
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .build()
    resizeClusters(config, clusterIds, numServeNodes, sleepDuration)
  }

  /**
   * Updates given clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at the
   * end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param clusterIds
   *   Names of clusters to be updated, all if empty
   * @param sleepDuration
   *   How long to sleep after updating the number of nodes. Google recommends at least 20 minutes
   *   before the new nodes are fully functional
   */
  def resizeClusters(
    config: CloudBigtableConfiguration,
    clusterIds: Set[String],
    numServeNodes: Int,
    sleepDuration: Duration
  ): Unit =
    if (!self.isTest) {
      InstanceAdmin.resizeClusters(config, clusterIds, numServeNodes, sleepDuration)
    }

  /**
   * Get size of all clusters for specified Bigtable instance.
   *
   * @return
   *   map of clusterId to its number of nodes
   */
  def getBigtableClusterSizes(projectId: String, instanceId: String): Map[String, Int] = {
    val config = new CloudBigtableConfiguration.Builder()
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .build()
    getBigtableClusterSizes(config)
  }

  /**
   * Get size of all clusters for specified Bigtable instance.
   *
   * @return
   *   map of clusterId to its number of nodes
   */
  def getBigtableClusterSizes(config: CloudBigtableConfiguration): Map[String, Int] =
    if (!self.isTest) {
      InstanceAdmin
        .listClusters(config)
        .map(c => c.getInstanceId -> c.getServeNodes)
        .toMap
    } else {
      Map.empty
    }

  /**
   * Ensure that tables and column families exist. Checks for existence of tables or creates them if
   * they do not exist. Also checks for existence of column families within each table and creates
   * them if they do not exist.
   *
   * @param tables
   */
  def ensureTables(
    projectId: String,
    instanceId: String,
    tables: Iterable[HTableDescriptor]
  ): Unit =
    ensureTables(projectId, instanceId, tables, TableAdmin.CreateDisposition.default)

  /**
   * Ensure that tables and column families exist. Checks for existence of tables or creates them if
   * they do not exist. Also checks for existence of column families within each table and creates
   * them if they do not exist.
   *
   * @param tables
   */
  def ensureTables(
    projectId: String,
    instanceId: String,
    tables: Iterable[HTableDescriptor],
    createDisposition: TableAdmin.CreateDisposition
  ): Unit = {
    val config = new CloudBigtableConfiguration.Builder()
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .build()
    ensureTables(config, tables, createDisposition)
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
  def ensureTables(
    config: CloudBigtableConfiguration,
    tables: Iterable[HTableDescriptor]
  ): Unit = ensureTables(config, tables, TableAdmin.CreateDisposition.default)

  def ensureTables(
    config: CloudBigtableConfiguration,
    tables: Iterable[HTableDescriptor],
    createDisposition: TableAdmin.CreateDisposition
  ): Unit = {
    if (!self.isTest) {
      TableAdmin.ensureTables(config, tables, createDisposition)
    }
  }
}

trait ScioContextSyntax {
  implicit def bigtableScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
