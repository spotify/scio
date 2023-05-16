/*
 * Copyright 2022 Spotify AB.
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

package com.spotify.scio.neo4j

import org.neo4j.driver.{SessionConfig, TransactionConfig}

/**
 * Options for a Neo4J connection.
 *
 * @param url
 *   connection url, e.g. "neo4j://neo4j.com:7687"
 * @param username
 *   login username
 * @param password
 *   login password
 */
final case class Neo4jConnectionOptions(url: String, username: String, password: String)

object Neo4jOptions {
  private[neo4j] val BeamDefaultSessionConfig = SessionConfig.defaultConfig
  private[neo4j] val BeamDefaultTransactionConfig = TransactionConfig.empty()
}

/**
 * Options for configuring a Neo4J driver.
 *
 * @param connectionOptions
 *   connection options
 * @param sessionConfig
 *   [[org.neo4j.driver.SessionConfig]] for specifying Neo4J driver session config
 * @param transactionConfig
 *   [[org.neo4j.driver.TransactionConfig]] for specifying Neo4J driver transaction config
 */
final case class Neo4jOptions(
  connectionOptions: Neo4jConnectionOptions,
  sessionConfig: SessionConfig = Neo4jOptions.BeamDefaultSessionConfig,
  transactionConfig: TransactionConfig = Neo4jOptions.BeamDefaultTransactionConfig
)
