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

import com.google.bigtable.admin.v2._
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest.Modification
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.BigtableTableAdminGrpcClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * Bigtable Table Admin API helper commands.
 */
object TableAdmin {
  private val log: Logger = LoggerFactory.getLogger(TableAdmin.getClass)

  /**
   * Ensure that tables and column families exist.
   * Checks for existence of tables or creates them if they do not exist.  Also checks for
   * existence of column families within each table and creates them if they do not exist.
   *
   * @param tablesAndColumnFamilies A map of tables and column families.  Keys are table names.
   *                                Values are a list of column family names.
   */
  def ensureTables(bigtableOptions: BigtableOptions,
                   tablesAndColumnFamilies: Map[String, List[String]]): Unit = {

    val channel = ChannelPoolCreator.createPool(bigtableOptions)
    val client = new BigtableTableAdminGrpcClient(channel)
    val project = bigtableOptions.getProjectId
    val instance = bigtableOptions.getInstanceId
    val instancePath = s"projects/$project/instances/$instance"

    log.info("Ensuring tables and column families exist in instance {}", instance)

    try {
      val existingTables = client.listTables(ListTablesRequest.newBuilder()
        .setParent(instancePath)
        .build())
        .getTablesList.asScala
        .map(t => t.getName).toSet

      for ((table, columnFamilies) <- tablesAndColumnFamilies) {
        val tablePath = s"$instancePath/tables/$table"

        if (!existingTables.contains(tablePath)) {
          log.info("Creating table {}", table)
          client.createTable(CreateTableRequest.newBuilder()
            .setParent(instancePath)
            .setTableId(table)
            .build())
        } else {
          log.info("Table {} exists", table)
        }

        ensureColumnFamilies(client, tablePath, columnFamilies)
      }
    } finally {
      channel.shutdownNow()
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
  private def ensureColumnFamilies(client: BigtableTableAdminGrpcClient,
                                   tablePath: String,
                                   columnFamilies: List[String]): Unit = {

    val tableInfo = client.getTable(GetTableRequest.newBuilder().setName(tablePath).build)

    val modifications: List[Modification] = columnFamilies.collect {
      case(cf) if !tableInfo.containsColumnFamilies(cf) =>
        Modification.newBuilder()
          .setId(cf)
          .setCreate(ColumnFamily.newBuilder())
          .build()
    }

    if (modifications.isEmpty) {
      log.info(s"Column families $columnFamilies exist in $tablePath")
    } else {
      log.info(s"Creating column families $columnFamilies in $tablePath")
      client.modifyColumnFamily(ModifyColumnFamiliesRequest.newBuilder()
        .setName(tablePath)
        .addAllModifications(modifications.asJava)
        .build)
    }
  }

}
