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

package com.spotify.scio.jdbc

import java.sql.Driver

/**
 * Options for a JDBC connection.
 *
 * @param username
 *   database login username
 * @param password
 *   database login password if exists
 * @param connectionUrl
 *   connection url, i.e "jdbc:mysql://[host]:[port]/db?"
 * @param driverClass
 *   subclass of [[java.sql.Driver]]
 */
final case class JdbcConnectionOptions(
  username: String,
  password: Option[String],
  connectionUrl: String,
  cloudSqlInstanceConnectionName: Option[String],
  driverClass: Class[_ <: Driver]
)

object JdbcConnectionOptions {
  def apply(
    username: String,
    password: Option[String],
    connectionUrl: String,
    driverClass: Class[_ <: Driver]
  ): JdbcConnectionOptions = {
    val cloudSqlInstance = getParameters(connectionUrl)
      .get("cloudSqlInstance")
      .flatten
    JdbcConnectionOptions(username, password, connectionUrl, cloudSqlInstance, driverClass)
  }

  def getParameters(connectionUrl: String): Map[String, Option[String]] = {
    connectionUrl.split('?').toList match {
      case _ :: parameters :: _ =>
        parameters
          .split('&')
          .map { x =>
            val pair = x.split('=')
            (pair(0), if (pair.length > 1) Some(pair(1)) else None)
          }
          .toMap
      case _ =>
        Map.empty
    }
  }
}
