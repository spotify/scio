/*
 * Copyright 2024 Spotify AB.
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

import com.google.cloud.bigtable.admin.v2.models.GCRules.GCRule
import com.google.cloud.bigtable.admin.v2.models.{
  CreateTableRequest,
  GCRules,
  ModifyColumnFamiliesRequest
}
import com.google.cloud.bigtable.admin.v2.{BigtableInstanceAdminClient, BigtableTableAdminClient}
import org.joda.time.Duration
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._

/**
 * Bigtable Table Admin API helper commands.
 *
 * Caches Bigtable clients and exposes basic operations
 */
object Admin {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  sys.addShutdownHook {
    logger.info("Shutting down Bigtable clients")
    Instance.clients.values.foreach(_.close())
    Table.clients.values.foreach(_.close())
  }

  object Table {

    sealed trait CreateDisposition
    object CreateDisposition {
      case object Never extends CreateDisposition
      case object CreateIfNeeded extends CreateDisposition

      val Default: CreateDisposition = CreateIfNeeded
    }

    private[bigtable] val clients: concurrent.Map[(String, String), BigtableTableAdminClient] =
      TrieMap.empty
    private def getOrCreateClient(
      projectId: String,
      instanceId: String
    ): BigtableTableAdminClient = {
      val key = (projectId, instanceId)
      clients.getOrElseUpdate(
        key,
        BigtableTableAdminClient.create(projectId, instanceId)
      )
    }

    /**
     * Retrieves a set of tables from the given instancePath.
     *
     * @param client
     *   Client for calling Bigtable.
     * @return
     */
    private def fetchTables(client: BigtableTableAdminClient): Set[String] =
      client.listTables().asScala.toSet

    /**
     * Ensure that tables and column families exist. Checks for existence of tables or creates them
     * if they do not exist. Also checks for existence of column families within each table and
     * creates them if they do not exist.
     *
     * @param tablesAndColumnFamilies
     *   A map of tables ids and column families. Values are a list of column family names.
     */
    def ensureTable(
      projectId: String,
      instanceId: String,
      tableId: String,
      columnFamilies: Iterable[String],
      createDisposition: CreateDisposition = CreateDisposition.Default
    ): Unit = {
      val tcf = columnFamilies.map(cf => cf -> None)
      ensureTableImpl(projectId, instanceId, tableId, tcf, createDisposition)
    }

    /**
     * Ensure that tables and column families exist. Checks for existence of tables or creates them
     * if they do not exist. Also checks for existence of column families within each table and
     * creates them if they do not exist.
     *
     * @param tablesAndColumnFamilies
     *   A map of table Ids and column families. Values are a list of column family names along with
     *   the desired cell expiration. Cell expiration is the duration before which garbage
     *   collection of a cell may occur. Note: minimum granularity is one second.
     */
    def ensureTablesWithExpiration(
      projectId: String,
      instanceId: String,
      tableId: String,
      columnFamilies: Iterable[(String, Option[Duration])],
      createDisposition: CreateDisposition = CreateDisposition.Default
    ): Unit = {
      // Convert Duration to GcRule
      val x = columnFamilies.map { case (columnFamily, duration) =>
        (columnFamily, duration.map(maxAgeGcRule))
      }
      ensureTableImpl(projectId, instanceId, tableId, x, createDisposition)
    }

    private def maxAgeGcRule(duration: Duration): GCRule =
      GCRules.GCRULES.maxAge(duration.getStandardSeconds, TimeUnit.SECONDS)

    /**
     * Ensure that tables and column families exist. Checks for existence of tables or creates them
     * if they do not exist. Also checks for existence of column families within each table and
     * creates them if they do not exist.
     *
     * @param tablesAndColumnFamilies
     *   A map of tables Id and column families. Values are a list of column family names along with
     *   the desired GcRule.
     */
    def ensureTableWithGcRules(
      projectId: String,
      instanceId: String,
      tableId: String,
      columnFamilies: Iterable[(String, Option[GCRule])],
      createDisposition: CreateDisposition = CreateDisposition.Default
    ): Unit = ensureTableImpl(projectId, instanceId, tableId, columnFamilies, createDisposition)

    /**
     * Ensure that tables and column families exist. Checks for existence of tables or creates them
     * if they do not exist. Also checks for existence of column families within each table and
     * creates them if they do not exist.
     *
     * @param tablesAndColumnFamilies
     *   A map of tables and column families. Keys are table names. Values are a list of column
     *   family names.
     */
    private def ensureTableImpl(
      projectId: String,
      instanceId: String,
      tableId: String,
      columnFamilies: Iterable[(String, Option[GCRule])],
      createDisposition: CreateDisposition
    ): Unit = {
      logger.info("Ensuring tables and column families exist in instance {}", instanceId)

      val client = getOrCreateClient(projectId, instanceId)
      val existingTables = fetchTables(client)
      val exists = existingTables.contains(tableId)
      if (exists) {
        logger.info("Table {} exists", tableId)
      } else {
        createDisposition match {
          case CreateDisposition.CreateIfNeeded =>
            logger.info("Creating table {}", tableId)
            client.createTable(CreateTableRequest.of(tableId))

            val table = client.getTable(tableId)
            val cf = table.getColumnFamilies.asScala.map(c => c.getId -> c).toMap

            val modifyRequest = columnFamilies.foldLeft(ModifyColumnFamiliesRequest.of(tableId)) {
              case (mr, (id, gcrOpt)) =>
                val gcRule = gcrOpt.getOrElse(GCRules.GCRULES.defaultRule())
                cf.get(id) match {
                  case None    => mr.addFamily(id, gcRule)
                  case Some(_) => mr.updateFamily(id, gcRule)
                }
            }

            logger.info("Modifying column families for table {}", tableId)
            client.modifyFamilies(modifyRequest)
          case CreateDisposition.Never =>
            throw new IllegalStateException(s"Table $tableId does not exist")
        }
      }
    }

    /**
     * Permanently deletes a row range from the specified table that match a particular prefix.
     *
     * @param table
     *   table name
     * @param rowPrefix
     *   row key prefix
     */
    def dropRowRange(
      projectId: String,
      instanceId: String,
      tableId: String,
      rowPrefix: String
    ): Unit = {
      val client = getOrCreateClient(projectId, instanceId)
      client.dropRowRange(tableId, rowPrefix)
    }
  }

  object Instance {
    private[bigtable] val clients: concurrent.Map[String, BigtableInstanceAdminClient] =
      TrieMap.empty
    private def getOrCreateClient(projectId: String): BigtableInstanceAdminClient = {
      clients.getOrElseUpdate(
        projectId,
        BigtableInstanceAdminClient.create(projectId)
      )
    }

    /**
     * Updates clusters within the specified Bigtable instance to a specified number of nodes.
     * Useful for increasing the number of nodes at the beginning of a job and decreasing it at the
     * end to lower costs yet still get high throughput during bulk ingests/dumps.
     *
     * @param numberOfNodes
     *   New number of nodes in the cluster
     * @param sleepDuration
     *   How long to sleep after updating the number of nodes. Google recommends at least 20 minutes
     *   before the new nodes are fully functional
     */
    def updateNumberOfBigtableNodes(
      projectId: String,
      instanceId: String,
      clusterIds: Set[String],
      numberOfNodes: Int,
      sleepDuration: Duration
    ): Unit = {
      val client = getOrCreateClient(projectId)
      val ids: Iterable[String] = if (clusterIds.isEmpty) {
        client.listClusters(instanceId).asScala.map(_.getId)
      } else {
        clusterIds
      }

      ids.foreach { clusterId =>
        logger.info("Updating number of nodes to {} for cluster {}", numberOfNodes, clusterId)
        client.resizeCluster(instanceId, clusterId, numberOfNodes)
      }

      if (sleepDuration.isLongerThan(Duration.ZERO)) {
        logger.info("Sleeping for {} after update", sleepDuration)
        Thread.sleep(sleepDuration.getMillis)
      }
    }

    /**
     * Get size of all clusters for specified Bigtable instance.
     *
     * @return
     *   map of clusterId to its number of nodes
     */
    def getClusterSizes(projectId: String, instanceId: String): Map[String, Int] = {
      val client = getOrCreateClient(projectId)
      client.listClusters(instanceId).asScala.map(c => c.getId -> c.getServeNodes).toMap
    }
  }
}
