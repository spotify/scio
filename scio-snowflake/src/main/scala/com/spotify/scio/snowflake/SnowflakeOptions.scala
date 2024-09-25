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

trait SnowflakeAuthenticationOptions

/**
 * Options for a Snowflake username/password authentication.
 *
 * @param username
 *   username
 * @param password
 *   password
 */
final case class SnowflakeUsernamePasswordAuthenticationOptions(
  username: String,
  password: String
) extends SnowflakeAuthenticationOptions

/**
 * Options for a Snowflake key pair authentication.
 *
 * @param username
 *   username
 * @param privateKeyPath
 *   path to the private key
 * @param privateKeyPassphrase
 *   passphrase for the private key (optional)
 */
final case class SnowflakeKeyPairAuthenticationOptions(
  username: String,
  privateKeyPath: String,
  privateKeyPassphrase: Option[String]
) extends SnowflakeAuthenticationOptions

/**
 * Options for a Snowflake OAuth token authentication.
 *
 * @param token
 *   OAuth token
 */
final case class SnowflakeOAuthTokenAuthenticationOptions(
  token: String
) extends SnowflakeAuthenticationOptions

/**
 * Options for a Snowflake connection.
 *
 * @param authenticationOptions
 *   authentication options
 * @param serverName
 *   server name (e.g. "account.region.snowflakecomputing.com")
 * @param database
 *   database name
 * @param role
 *   role name
 * @param warehouse
 *   warehouse name
 * @param schema
 *   schema name (optional)
 */
final case class SnowflakeConnectionOptions(
  authenticationOptions: SnowflakeAuthenticationOptions,
  serverName: String,
  database: String,
  role: String,
  warehouse: String,
  schema: Option[String]
)

/**
 * Options for configuring a Neo4J driver.
 *
 * @param connectionOptions
 *   connection options
 * @param stagingBucketName
 *   Snowflake staging bucket name where CSV files will be stored
 * @param storageIntegrationName
 *   Storage integration name as created in Snowflake
 */
final case class SnowflakeOptions(
  connectionOptions: SnowflakeConnectionOptions,
  stagingBucketName: String,
  storageIntegrationName: String
)
