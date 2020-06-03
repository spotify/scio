/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.jdbc.sharded

import java.sql.{Connection, DriverManager}

import com.spotify.scio.jdbc.JdbcConnectionOptions
import org.slf4j.LoggerFactory

private[jdbc] object JdbcUtils {

  private val IndexInfoTableNameField = "COLUMN_NAME"
  private val log = LoggerFactory.getLogger(this.getClass)

  def createConnection(connectionOptions: JdbcConnectionOptions): Connection = {
    val connection = DriverManager.getConnection(
      connectionOptions.connectionUrl,
      connectionOptions.username,
      connectionOptions.password.get
    )
    connection.setAutoCommit(false)
    log.info("Created connection to [{}]", connectionOptions.connectionUrl)

    connection
  }

  def getIndexedColumns(connection: Connection, tableName: String): Iterator[String] = {
    val dbMetadata = connection.getMetaData

    val resultSet = dbMetadata.getIndexInfo(null, null, tableName, false, false)

    new Iterator[String] {
      def hasNext: Boolean = resultSet.next()

      def next(): String = resultSet.getString(IndexInfoTableNameField)
    }.flatMap(Option(_))
  }

}
