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

package com.spotify.scio.jdbc

object SqlServer {
  val projectId = "data-integration-test"
  val regionId = "us-central1"
  val sqlServerInstanceId = "scio-sql-server-it"
  val databaseId = "shard-it"
  val username = "sqlserver"
  val password = sys.props.get("cloudsql.sqlserver.password")

  val connection = JdbcConnectionOptions(
    username,
    password,
    s"jdbc:sqlserver://localhost;" +
      "socketFactoryClass=com.google.cloud.sql.sqlserver.SocketFactory;" +
      s"socketFactoryConstructorArg=$projectId:$regionId:$sqlServerInstanceId;" +
      s"databaseName=$databaseId;" +
      "encrypt=false", // otherwise we'll have to generate certificates
    classOf[com.microsoft.sqlserver.jdbc.SQLServerDriver]
  )
}
