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

package com.spotify.scio.snowflake

import org.joda.time.Duration

sealed trait SnowflakeAuthenticationOptions

object SnowflakeAuthenticationOptions {

  /**
   * Snowflake username/password authentication.
   *
   * @param username
   *   username
   * @param password
   *   password
   */
  final case class UsernamePassword(
    username: String,
    password: String
  ) extends SnowflakeAuthenticationOptions

  /**
   * Key pair authentication.
   *
   * @param username
   *   username
   * @param privateKeyPath
   *   path to the private key
   * @param privateKeyPassphrase
   *   passphrase for the private key (optional)
   */
  final case class KeyPair(
    username: String,
    privateKeyPath: String,
    privateKeyPassphrase: Option[String] = None
  ) extends SnowflakeAuthenticationOptions

  /**
   * OAuth token authentication.
   *
   * @param token
   *   OAuth token
   */
  final case class OAuthToken(token: String) extends SnowflakeAuthenticationOptions

}

/**
 * Options for a Snowflake connection.
 *
 * @param authenticationOptions
 *   authentication options
 * @param url
 *   Sets URL of Snowflake server in following format:
 *   "jdbc:snowflake://[host]:[port].snowflakecomputing.com"
 * @param database
 *   database to use
 * @param role
 *   user's role to be used when running queries on Snowflake
 * @param warehouse
 *   warehouse name
 * @param schema
 *   schema to use when connecting to Snowflake
 * @param loginTimeout
 *   Sets loginTimeout that will be used in [[net.snowflake.client.jdbc.SnowflakeBasicDataSource]].
 */
final case class SnowflakeConnectionOptions(
  url: String,
  authenticationOptions: SnowflakeAuthenticationOptions = null,
  database: String = null,
  role: String = null,
  warehouse: String = null,
  schema: String = null,
  loginTimeout: Duration = null
)
