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

import com.spotify.scio.ScioContext
import com.spotify.scio.jdbc._

import scala.language.existentials

// Read from Google Cloud SQL database table and write to a different table in the same database
object CloudSqlExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    // Ensure this has been called with required database connection details
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](cmdlineArgs)

    val sc = ScioContext(opts)
    val connOptions = getConnectionOptions(opts)
    val readOptions = getReadOptions(connOptions)
    val writeOptions = getWriteOptions(connOptions)

    sc.jdbcSelect(readOptions)
      .map(kv => (kv._1.toUpperCase, kv._2))
      .saveAsJdbc(writeOptions)
    sc.close()
  }

  def getJdbcUrl(opts: CloudSqlOptions): String = {
    // socketFactory=com.google.cloud.sql.mysql.SocketFactory enables a secure connection to a
    // Cloud SQL instance using Cloud SDK credential. With this option you don't need to white
    // list your IP to access the database.
    // For more details: https://cloud.google.com/sql/docs/mysql/connect-external-app#java
    // For other options: https://cloud.google.com/sql/docs/mysql/external-connection-methods
    // For more information on socketFactory:
    // https://github.com/GoogleCloudPlatform/cloud-sql-mysql-socket-factory
    s"jdbc:mysql://google/${opts.getCloudSqlDb}?" +
      s"cloudSqlInstance=${opts.getCloudSqlInstanceConnectionName}&" +
      s"socketFactory=com.google.cloud.sql.mysql.SocketFactory"
  }

  // Basic connection details
  def getConnectionOptions(opts: CloudSqlOptions): JdbcConnectionOptions =
    JdbcConnectionOptions(
      username = opts.getCloudSqlUsername,
      password = opts.getCloudSqlPassword,
      driverClass = classOf[com.mysql.jdbc.Driver],
      connectionUrl = getJdbcUrl(opts))

  // Read from a table called `word_count` with two columns 'word' and 'count'
  def getReadOptions(connOpts: JdbcConnectionOptions): JdbcReadOptions[(String, Long)] =
    JdbcReadOptions(
      connectionOptions = connOpts,
      query = "SELECT * FROM word_count",
      rowMapper = r => (r.getString(1), r.getLong(2)))

  // Write to a table called `result_word_count` with two columns 'word' and 'count'
  def getWriteOptions(connOpts: JdbcConnectionOptions): JdbcWriteOptions[(String, Long)] =
    JdbcWriteOptions(
      connectionOptions = connOpts,
      statement = "INSERT INTO result_word_count values(?, ?)",
      preparedStatementSetter = (kv, s) => {
        s.setString(1, kv._1)
        s.setLong(2, kv._2)
      })

}
