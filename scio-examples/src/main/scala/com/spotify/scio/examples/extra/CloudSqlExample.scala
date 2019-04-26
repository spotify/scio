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

// Example: Cloud SQL Input and Output
package com.spotify.scio.examples.extra

import com.spotify.scio.ScioContext
import com.spotify.scio.jdbc._

// Read from Google Cloud SQL database table and write to a different table in the same database
object CloudSqlExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    // Parse database connection details as `CloudSqlOptions`
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](cmdlineArgs)

    val sc = ScioContext(opts)
    val connOptions = getConnectionOptions(opts)
    val readOptions = getReadOptions(connOptions)
    val writeOptions = getWriteOptions(connOptions)

    // Read from Cloud SQL
    sc.jdbcSelect(readOptions)
      .map(kv => (kv._1.toUpperCase, kv._2))
      // Write to Cloud SQL
      .saveAsJdbc(writeOptions)
    sc.close()
    ()
  }

  // `socketFactory=com.google.cloud.sql.mysql.SocketFactory` enables a secure connection to a
  // Cloud SQL instance using Cloud SDK credential. With this option you don't need to white
  // list your IP to access the database.
  //
  // - See this [page](https://cloud.google.com/sql/docs/mysql/connect-external-app#java) for more
  // details
  // - See this [page](https://cloud.google.com/sql/docs/mysql/external-connection-methods) for
  // other options
  // - See this [page](https://github.com/GoogleCloudPlatform/cloud-sql-mysql-socket-factory) for
  // more information on socket factory
  def getJdbcUrl(opts: CloudSqlOptions): String = {
    s"jdbc:mysql://google/${opts.getCloudSqlDb}?" +
      s"cloudSqlInstance=${opts.getCloudSqlInstanceConnectionName}&" +
      s"socketFactory=com.google.cloud.sql.mysql.SocketFactory"
  }

  // Basic connection details
  def getConnectionOptions(opts: CloudSqlOptions): JdbcConnectionOptions =
    JdbcConnectionOptions(username = opts.getCloudSqlUsername,
                          password = Some(opts.getCloudSqlPassword),
                          driverClass = classOf[com.mysql.jdbc.Driver],
                          connectionUrl = getJdbcUrl(opts))

  // Read from a table called `word_count` with two columns `word` and `count`
  def getReadOptions(connOpts: JdbcConnectionOptions): JdbcReadOptions[(String, Long)] =
    JdbcReadOptions(connectionOptions = connOpts,
                    query = "SELECT * FROM word_count",
                    rowMapper = r => (r.getString(1), r.getLong(2)))

  // Write to a table called `result_word_count` with two columns `word` and `count`
  def getWriteOptions(connOpts: JdbcConnectionOptions): JdbcWriteOptions[(String, Long)] =
    JdbcWriteOptions(
      connectionOptions = connOpts,
      statement = "INSERT INTO result_word_count values(?, ?)",
      preparedStatementSetter = (kv, s) => {
        s.setString(1, kv._1)
        s.setLong(2, kv._2)
      }
    )

}
