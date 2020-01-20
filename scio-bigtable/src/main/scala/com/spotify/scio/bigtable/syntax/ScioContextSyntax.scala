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

import com.google.bigtable.admin.v2.GcRule
import com.google.bigtable.v2._
import com.google.cloud.bigtable.config.BigtableOptions
import com.spotify.scio.ScioContext
import com.spotify.scio.bigtable.BigtableRead
import com.spotify.scio.bigtable.BigtableUtil
import com.spotify.scio.bigtable.TableAdmin
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.joda.time.Duration

import scala.collection.JavaConverters._

object ScioContextOps {
  private val DefaultSleepDuration = Duration.standardMinutes(20)
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
    rowFilter: RowFilter = BigtableRead.ReadParam.DefaultRowFilter
  ): SCollection[Row] = {
    val parameters = BigtableRead.ReadParam(keyRange, rowFilter)
    self.read(BigtableRead(projectId, instanceId, tableId))(parameters)
  }

  /** Get an SCollection for a Bigtable table. */
  def bigtable(
    bigtableOptions: BigtableOptions,
    tableId: String,
    keyRange: ByteKeyRange,
    rowFilter: RowFilter
  ): SCollection[Row] = {
    val parameters = BigtableRead.ReadParam(keyRange, rowFilter)
    self.read(BigtableRead(bigtableOptions, tableId))(parameters)
  }

  /**
   * Updates all clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at
   * the end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param sleepDuration How long to sleep after updating the number of nodes. Google recommends
   *                      at least 20 minutes before the new nodes are fully functional
   */
  def updateNumberOfBigtableNodes(
    projectId: String,
    instanceId: String,
    numberOfNodes: Int,
    sleepDuration: Duration = DefaultSleepDuration
  ): Unit = {
    val bigtableOptions = BigtableOptions
      .builder()
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .build
    updateNumberOfBigtableNodes(bigtableOptions, numberOfNodes, sleepDuration)
  }

  /**
   * Updates all clusters within the specified Bigtable instance to a specified number of nodes.
   * Useful for increasing the number of nodes at the beginning of a job and decreasing it at
   * the end to lower costs yet still get high throughput during bulk ingests/dumps.
   *
   * @param sleepDuration How long to sleep after updating the number of nodes. Google recommends
   *                      at least 20 minutes before the new nodes are fully functional
   */
  def updateNumberOfBigtableNodes(
    bigtableOptions: BigtableOptions,
    numberOfNodes: Int,
    sleepDuration: Duration
  ): Unit =
    if (!self.isTest) {
      // No need to update the number of nodes in a test
      BigtableUtil.updateNumberOfBigtableNodes(bigtableOptions, numberOfNodes, sleepDuration)
    }

  /**
   * Get size of all clusters for specified Bigtable instance.
   *
   * @return map of clusterId to its number of nodes
   */
  def getBigtableClusterSizes(projectId: String, instanceId: String): Map[String, Int] =
    if (!self.isTest) {
      BigtableUtil
        .getClusterSizes(projectId, instanceId)
        .asScala
        .iterator
        .map { case (k, v) => k -> v.toInt }
        .toMap
    } else {
      Map.empty
    }

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamilies A map of tables and column families.  Keys are table names.
   *                                Values are a list of column family names.
   */
  def ensureTables(
    projectId: String,
    instanceId: String,
    tablesAndColumnFamilies: Map[String, List[String]]
  ): Unit =
    if (!self.isTest) {
      val bigtableOptions = BigtableOptions
        .builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build
      TableAdmin.ensureTables(bigtableOptions, tablesAndColumnFamilies)
    }

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamilies A map of tables and column families.  Keys are table names.
   *                                Values are a list of column family names.
   */
  def ensureTables(
    bigtableOptions: BigtableOptions,
    tablesAndColumnFamilies: Map[String, List[String]]
  ): Unit =
    if (!self.isTest) {
      TableAdmin.ensureTables(bigtableOptions, tablesAndColumnFamilies)
    }

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamiliesWithExpiration A map of tables and column families.
   *                                              Keys are table names. Values are a
   *                                              list of column family names along with
   *                                              the desired cell expiration. Cell
   *                                              expiration is the duration before which
   *                                              garbage collection of a cell may occur.
   *                                              Note: minimum granularity is second.
   */
  def ensureTablesWithExpiration(
    projectId: String,
    instanceId: String,
    tablesAndColumnFamiliesWithExpiration: Map[String, List[(String, Option[Duration])]]
  ): Unit =
    if (!self.isTest) {
      val bigtableOptions = BigtableOptions
        .builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build
      TableAdmin.ensureTablesWithExpiration(
        bigtableOptions,
        tablesAndColumnFamiliesWithExpiration
      )
    }

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamiliesWithExpiration A map of tables and column families.
   *                                              Keys are table names. Values are a
   *                                              list of column family names along with
   *                                              the desired cell expiration. Cell
   *                                              expiration is the duration before which
   *                                              garbage collection of a cell may occur.
   *                                              Note: minimum granularity is second.
   */
  def ensureTablesWithExpiration(
    bigtableOptions: BigtableOptions,
    tablesAndColumnFamiliesWithExpiration: Map[String, List[(String, Option[Duration])]]
  ): Unit =
    if (!self.isTest) {
      TableAdmin.ensureTablesWithExpiration(
        bigtableOptions,
        tablesAndColumnFamiliesWithExpiration
      )
    }

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamiliesWithGcRules A map of tables and column families. Keys are
   *                                           table names. Values are a list of column family
   *                                           names along with the desired GcRule.
   */
  def ensureTablesWithGcRules(
    projectId: String,
    instanceId: String,
    tablesAndColumnFamiliesWithGcRules: Map[String, List[(String, Option[GcRule])]]
  ): Unit =
    if (!self.isTest) {
      val bigtableOptions = BigtableOptions
        .builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build
      TableAdmin.ensureTablesWithGcRules(
        bigtableOptions,
        tablesAndColumnFamiliesWithGcRules
      )
    }

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamiliesWithGcRule A map of tables and column families.
   *                                          Keys are table names. Values are a
   *                                          list of column family names along with
   *                                          the desired cell expiration. Cell
   *                                          expiration is the duration before which
   *                                          garbage collection of a cell may occur.
   *                                          Note: minimum granularity is second.
   */
  def ensureTablesWithGcRules(
    bigtableOptions: BigtableOptions,
    tablesAndColumnFamiliesWithGcRule: Map[String, List[(String, Option[GcRule])]]
  ): Unit =
    if (!self.isTest) {
      TableAdmin.ensureTablesWithGcRules(
        bigtableOptions,
        tablesAndColumnFamiliesWithGcRule
      )
    }

}

trait ScioContextSyntax {
  implicit def bigtableScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
