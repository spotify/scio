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

import org.apache.beam.sdk.io.jdbc.JdbcIO.RetryConfiguration

import java.sql.{PreparedStatement, ResultSet, SQLException}

@deprecated("Use new API overloads with multiple parameters", since = "0.13.0")
sealed trait JdbcIoOptions

/**
 * Options for reading from a JDBC source.
 *
 * @param connectionOptions
 *   connection options
 * @param query
 *   query string
 * @param statementPreparator
 *   function to prepare a [[java.sql.PreparedStatement]]
 * @param rowMapper
 *   function to map from a SQL [[java.sql.ResultSet]] to `T`
 * @param fetchSize
 *   use apache beam default fetch size if the value is -1
 * @param outputParallelization
 *   reshuffle result to distribute it to all workers. Default to true.
 */
@deprecated("Use new API overloads with multiple parameters", since = "0.13.0")
final case class JdbcReadOptions[T](
  connectionOptions: JdbcConnectionOptions,
  query: String,
  statementPreparator: PreparedStatement => Unit = null,
  rowMapper: ResultSet => T,
  fetchSize: Int = JdbcIO.ReadParam.BeamDefaultFetchSize,
  outputParallelization: Boolean = JdbcIO.ReadParam.DefaultOutputParallelization
) extends JdbcIoOptions

/**
 * Options for writing to a JDBC source.
 *
 * @param connectionOptions
 *   connection options
 * @param statement
 *   query statement
 * @param preparedStatementSetter
 *   function to set values in a [[java.sql.PreparedStatement]]
 * @param batchSize
 *   use apache beam default batch size if the value is -1
 * @param retryConfiguration
 *   [[org.apache.beam.sdk.io.jdbc.JdbcIO.RetryConfiguration]] for specifying retry behavior
 * @param retryStrategy
 *   A predicate of [[java.sql.SQLException]] indicating a failure to retry
 */
@deprecated("Use new API overloads with multiple parameters", since = "0.13.0")
final case class JdbcWriteOptions[T](
  connectionOptions: JdbcConnectionOptions,
  statement: String,
  preparedStatementSetter: (T, PreparedStatement) => Unit = null,
  batchSize: Long = JdbcIO.WriteParam.BeamDefaultBatchSize,
  retryConfiguration: RetryConfiguration = JdbcIO.WriteParam.BeamDefaultRetryConfiguration,
  retryStrategy: SQLException => Boolean = JdbcIO.WriteParam.DefaultRetryStrategy
) extends JdbcIoOptions
