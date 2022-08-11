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

import org.neo4j.driver.{Record, SessionConfig, TransactionConfig}

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

sealed trait Neo4jIoOptions {
  def connectionOptions: Neo4jConnectionOptions
  def cypher: String
}

object Neo4jIoOptions {
  private[neo4j] val BeamDefaultBatchSize = 5000L
  private[neo4j] val BeamDefaultSessionConfig = SessionConfig.defaultConfig
  private[neo4j] val BeamDefaultTransactionConfig = TransactionConfig.empty()
}

/**
 * Options for reading from a Neo4J source.
 *
 * @param connectionOptions
 *   connection options
 * @param cypher
 *   cypher query
 * @param rowMapper
 *   map from [[org.neo4j.driver.Record]] to the target entity
 * @param sessionConfig
 *   [[org.neo4j.driver.SessionConfig]] for specifying Neo4J driver session config
 * @param transactionConfig
 *   [[org.neo4j.driver.TransactionConfig]] for specifying Neo4J driver transaction config
 */
final case class Neo4jReadOptions[T](
  connectionOptions: Neo4jConnectionOptions,
  cypher: String,
  rowMapper: Record => T,
  sessionConfig: SessionConfig = Neo4jIoOptions.BeamDefaultSessionConfig,
  transactionConfig: TransactionConfig = Neo4jIoOptions.BeamDefaultTransactionConfig
) extends Neo4jIoOptions

/**
 * Options for writing to a Neo4J source.
 *
 * @param connectionOptions
 *   connection options
 * @param cypher
 *   cypher query, that should start with UNWIND $rows AS row
 * @param unwindMapName
 *   var name used by UNWIND, e.g. "rows"
 * @param parametersFunction
 *   map from entities that should be written to name parameters used by the cypher query
 * @param batchSize
 *   use apache beam default batch size if the value is -1
 * @param sessionConfig
 *   [[org.neo4j.driver.SessionConfig]] for specifying Neo4J driver session config
 * @param transactionConfig
 *   [[org.neo4j.driver.TransactionConfig]] for specifying Neo4J driver transaction config
 */
final case class Neo4jWriteOptions[T](
  connectionOptions: Neo4jConnectionOptions,
  cypher: String,
  unwindMapName: String,
  parametersFunction: T => Map[String, AnyRef],
  batchSize: Long = Neo4jIoOptions.BeamDefaultBatchSize,
  sessionConfig: SessionConfig = Neo4jIoOptions.BeamDefaultSessionConfig,
  transactionConfig: TransactionConfig = Neo4jIoOptions.BeamDefaultTransactionConfig
) extends Neo4jIoOptions
