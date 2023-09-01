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

import com.google.cloud.bigtable.beam.{CloudBigtableConfiguration, CloudBigtableTableConfiguration}
import com.google.cloud.bigtable.hbase.BigtableConfiguration
import org.apache.hadoop.hbase.client.AbstractBigtableAdmin
import org.apache.hadoop.hbase.{HTableDescriptor, TableName}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets

/** Bigtable Table Admin API helper commands. */
object TableAdmin {

  sealed trait CreateDisposition
  object CreateDisposition {
    case object Never extends CreateDisposition
    case object CreateIfNeeded extends CreateDisposition
    def default: CreateDisposition = CreateIfNeeded
  }

  private val log: Logger = LoggerFactory.getLogger(TableAdmin.getClass)

  private def execute[A](
    config: CloudBigtableConfiguration
  )(f: AbstractBigtableAdmin => A): A = {
    val connection = BigtableConfiguration.connect(config.toHBaseConfig)
    try {
      f(connection.getAdmin.asInstanceOf[AbstractBigtableAdmin])
    } finally {
      connection.close()
    }
  }

  /**
   * Ensure that tables and column families exist. Checks for existence of tables or creates them if
   * they do not exist. Also checks for existence of column families within each table and creates
   * them if they do not exist.
   *
   * @param config
   *   Bigtable configuration
   * @param tables
   *   List of tables to check existence
   * @param createDisposition
   *   Create disposition One of [CreateIfNeeded, Never]
   */
  def ensureTables(
    config: CloudBigtableConfiguration,
    tables: Iterable[HTableDescriptor],
    createDisposition: CreateDisposition = CreateDisposition.default
  ): Unit = {
    log.info("Ensuring tables and column families exist in instance {}", config.getInstanceId)
    execute(config) { client =>
      val existingTables = client.listTableNames().toSet
      tables.foreach { table =>
        val tableName = table.getTableName
        val exists = existingTables.contains(tableName)
        createDisposition match {
          case _ if exists =>
            log.info("Table {} exists", tableName)
            val existingTable = client.getTableDescriptor(tableName)
            if (existingTable != table) {
              log.info("Modifying table {}", tableName)
              client.modifyTable(tableName, table)
            }
          case CreateDisposition.CreateIfNeeded =>
            log.info("Creating table {}", tableName)
            client.createTable(table)
          case CreateDisposition.Never =>
            throw new IllegalStateException(s"Table $tableName does not exist")
        }
      }
    }
  }

  /**
   * Permanently deletes a row range from the specified table that match a particular prefix.
   *
   * @param config
   *   Bigtable configuration
   * @param tableName
   *   Table name
   * @param prefix
   *   Row key prefix
   */
  def dropRowRange(
    config: CloudBigtableTableConfiguration,
    tableName: TableName,
    prefix: String
  ): Unit =
    execute(config)(_.deleteRowRangeByPrefix(tableName, prefix.getBytes(StandardCharsets.UTF_8)))
}
