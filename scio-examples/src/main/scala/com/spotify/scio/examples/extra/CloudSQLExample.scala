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

package com.spotify.scio.examples.extra

import java.sql.{PreparedStatement, ResultSet}

import com.spotify.scio.jdbc._
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.examples.common.ExampleCloudSQLOptions
import org.apache.beam.sdk.values.KV

// Read from google Cloud SQL database table and write to different table in the same database.
object CloudSQLExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Ensure this has been called with required database connection details.
    val (cloudSqlOptions, extraArgs) =
      ScioContext.parseArguments[ExampleCloudSQLOptions](cmdlineArgs)

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val connOpt = getDbConnectionOptions(cloudSqlOptions)
    val readOptions = getReadOptions(connOpt)
    val writeOptions = getWriteOptions(connOpt)

    sc.withName("Read from Cloud Sql").jdbcSelect(readOptions)
      .map(kv => KV.of(kv.getKey + "_", kv.getValue)) // append _ to words
      .withName("Write to Cloud Sql").saveAsJdbc(writeOptions)
    sc.close()
  }

  def getJdbcUrl(options: ExampleCloudSQLOptions): String = {
    // socketFactory:com.google.cloud.sql.mysql.SocketFactory enable to create a secure connection
    // with cloud sql instance using Cloud SDK credential. With this option you don't need to
    // white list your ip to get access to database.
    // for more details look: https://cloud.google.com/sql/docs/mysql/connect-external-app#java
    // for other options look: https://cloud.google.com/sql/docs/mysql/external-connection-methods
    // look: https://github.com/GoogleCloudPlatform/cloud-sql-mysql-socket-factory for socketFactory
    s"jdbc:mysql://google/${options.getCloudSqlDb}?" +
      s"cloudSqlInstance=${options.getCloudSqlInstanceConnectionName}&" +
      s"socketFactory=com.google.cloud.sql.mysql.SocketFactory"
  }

  def getDbConnectionOptions(cloudSqlOptions: ExampleCloudSQLOptions): DbConnectionOptions = {
    // Basic connection details
    DbConnectionOptions(
      username = "root",
      password = "bqstats",
      driverClass = classOf[com.mysql.jdbc.Driver],
      connectionUrl = getJdbcUrl(cloudSqlOptions))
  }

  def getReadOptions(connOpts: DbConnectionOptions): JdbcReadOptions[KV[String, Long]] = {
    // Read from a table called `word_count` which has two columns 'word' and 'count'
    JdbcReadOptions(
      dbConnectionOptions = connOpts,
      query = "SELECT * FROM word_count",
      statementPreparator = (preparedStatement: PreparedStatement) => {},
      rowMapper = (resultSet: ResultSet) => {
        KV.of(resultSet.getString(1), resultSet.getLong(2))
      }
    )
  }

  def getWriteOptions(connOpt: DbConnectionOptions): JdbcWriteOptions[KV[String, Long]] = {
    // Write to a table called `result_word_count` which has two columns 'word' and 'count'
    JdbcWriteOptions(
      dbConnectionOptions = connOpt,
      statement = "INSERT INTO result_word_count values(?, ?)",
      preparedStatementSetter = (kv: KV[String, Long], preparedStatement: PreparedStatement) => {
        preparedStatement.setString(1, kv.getKey)
        preparedStatement.setLong(2, kv.getValue)
      }
    )
  }

}
