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

import java.nio.charset.Charset

import com.google.bigtable.admin.v2._
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc._
import com.google.protobuf.{ByteString, Duration => ProtoDuration}
import org.joda.time.Duration
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._
import scala.util.Try

/** Bigtable Table Admin API helper commands. */
object TableAdmin {

  sealed trait CreateDisposition
  object CreateDisposition {
    case object Never extends CreateDisposition
    case object CreateIfNeeded extends CreateDisposition
    val default = CreateIfNeeded
  }

  private val log: Logger = LoggerFactory.getLogger(TableAdmin.getClass)

  private def adminClient[A](
    bigtableOptions: BigtableOptions
  )(f: BigtableTableAdminClient => A): Try[A] = {
    val channel =
      ChannelPoolCreator.createPool(bigtableOptions)
    val executorService =
      BigtableSessionSharedThreadPools.getInstance().getRetryExecutor
    val client = new BigtableTableAdminGrpcClient(channel, executorService, bigtableOptions)

    val result = Try(f(client))
    channel.shutdownNow()
    result
  }

  /**
   * Retrieves a set of tables from the given instancePath.
   *
   * @param client Client for calling Bigtable.
   * @param instancePath String of the form "projects/$project/instances/$instance".
   * @return
   */
  private def fetchTables(client: BigtableTableAdminClient, instancePath: String): Set[String] =
    client
      .listTables(
        ListTablesRequest
          .newBuilder()
          .setParent(instancePath)
          .build()
      )
      .getTablesList
      .asScala
      .map(_.getName)
      .toSet

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
    tablesAndColumnFamilies: Map[String, Iterable[String]],
    createDisposition: CreateDisposition = CreateDisposition.default
  ): Unit = {
    val tcf = tablesAndColumnFamilies.iterator.map { case (k, l) =>
      k -> l.map(_ -> None)
    }.toMap
    ensureTablesImpl(bigtableOptions, tcf, createDisposition).get
  }

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamilies A map of tables and column families.
   *                                Keys are table names. Values are a
   *                                list of column family names along with
   *                                the desired cell expiration. Cell
   *                                expiration is the duration before which
   *                                garbage collection of a cell may occur.
   *                                Note: minimum granularity is one second.
   */
  def ensureTablesWithExpiration(
    bigtableOptions: BigtableOptions,
    tablesAndColumnFamilies: Map[String, Iterable[(String, Option[Duration])]],
    createDisposition: CreateDisposition = CreateDisposition.default
  ): Unit = {
    // Convert Duration to GcRule
    val x = tablesAndColumnFamilies.iterator.map { case (k, v) =>
      k -> v.map { case (columnFamily, duration) =>
        (columnFamily, duration.map(gcRuleFromDuration))
      }
    }.toMap

    ensureTablesImpl(bigtableOptions, x, createDisposition).get
  }

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamilies A map of tables and column families. Keys are table names.
   *                                Values are a list of column family names along with the desired
   *                                GcRule.
   */
  def ensureTablesWithGcRules(
    bigtableOptions: BigtableOptions,
    tablesAndColumnFamilies: Map[String, Iterable[(String, Option[GcRule])]],
    createDisposition: CreateDisposition = CreateDisposition.default
  ): Unit =
    ensureTablesImpl(bigtableOptions, tablesAndColumnFamilies, createDisposition).get

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamilies A map of tables and column families.  Keys are table names.
   *                                Values are a list of column family names.
   */
  private def ensureTablesImpl(
    bigtableOptions: BigtableOptions,
    tablesAndColumnFamilies: Map[String, Iterable[(String, Option[GcRule])]],
    createDisposition: CreateDisposition
  ): Try[Unit] = {
    val project = bigtableOptions.getProjectId
    val instance = bigtableOptions.getInstanceId
    val instancePath = s"projects/$project/instances/$instance"

    log.info("Ensuring tables and column families exist in instance {}", instance)

    adminClient(bigtableOptions) { client =>
      val existingTables = fetchTables(client, instancePath)

      tablesAndColumnFamilies.foreach { case (table, columnFamilies) =>
        val tablePath = s"$instancePath/tables/$table"

        val exists = existingTables.contains(tablePath)
        createDisposition match {
          case _ if exists =>
            log.info("Table {} exists", table)
          case CreateDisposition.CreateIfNeeded =>
            log.info("Creating table {}", table)
            client.createTable(
              CreateTableRequest
                .newBuilder()
                .setParent(instancePath)
                .setTableId(table)
                .build()
            )
          case CreateDisposition.Never =>
            throw new IllegalStateException(s"Table $table does not exist")
        }

        ensureColumnFamilies(client, tablePath, columnFamilies, createDisposition)
      }
    }
  }

  /**
   * Ensure that column families exist.
   * Checks for existence of column families and creates them if they don't exist.
   *
   * @param tablePath A full table path that the bigtable API expects, in the form of
   *                  `projects/projectId/instances/instanceId/tables/tableId`
   * @param columnFamilies A list of column family names.
   */
  private def ensureColumnFamilies(
    client: BigtableTableAdminClient,
    tablePath: String,
    columnFamilies: Iterable[(String, Option[GcRule])],
    createDisposition: CreateDisposition
  ): Unit =
    createDisposition match {
      case CreateDisposition.CreateIfNeeded =>
        val tableInfo =
          client.getTable(GetTableRequest.newBuilder().setName(tablePath).build)

        val cfList = columnFamilies
          .map { case (n, gcRule) =>
            val cf = tableInfo
              .getColumnFamiliesOrDefault(n, ColumnFamily.newBuilder().build())
              .toBuilder
              .setGcRule(gcRule.getOrElse(GcRule.getDefaultInstance))
              .build()

            (n, cf)
          }
        val modifications =
          cfList.map { case (n, cf) =>
            val mod = Modification.newBuilder().setId(n)
            if (tableInfo.containsColumnFamilies(n)) {
              mod.setUpdate(cf)
            } else {
              mod.setCreate(cf)
            }
            mod.build()
          }

        log.info(
          "Modifying or updating {} column families for table {}",
          modifications.size,
          tablePath
        )

        if (modifications.nonEmpty) {
          client.modifyColumnFamily(
            ModifyColumnFamiliesRequest
              .newBuilder()
              .setName(tablePath)
              .addAllModifications(modifications.asJava)
              .build
          )
        }
        ()
      case CreateDisposition.Never =>
        ()
    }

  private def gcRuleFromDuration(duration: Duration): GcRule = {
    val protoDuration = ProtoDuration.newBuilder.setSeconds(duration.getStandardSeconds)
    GcRule.newBuilder.setMaxAge(protoDuration).build
  }

  /**
   * Permanently deletes a row range from the specified table that match a particular prefix.
   *
   * @param table table name
   * @param rowPrefix row key prefix
   */
  def dropRowRange(bigtableOptions: BigtableOptions, table: String, rowPrefix: String): Try[Unit] =
    adminClient(bigtableOptions) { client =>
      val project = bigtableOptions.getProjectId
      val instance = bigtableOptions.getInstanceId
      val instancePath = s"projects/$project/instances/$instance"
      val tablePath = s"$instancePath/tables/$table"

      dropRowRange(tablePath, rowPrefix, client)
    }

  /**
   * Permanently deletes a row range from the specified table that match a particular prefix.
   *
   * @param tablePath A full table path that the bigtable API expects, in the form of
   *                  `projects/projectId/instances/instanceId/tables/tableId`
   * @param rowPrefix row key prefix
   */
  private def dropRowRange(
    tablePath: String,
    rowPrefix: String,
    client: BigtableTableAdminClient
  ): Unit = {
    val request = DropRowRangeRequest
      .newBuilder()
      .setName(tablePath)
      .setRowKeyPrefix(ByteString.copyFrom(rowPrefix, Charset.forName("UTF-8")))
      .build()

    client.dropRowRange(request)
  }
}
