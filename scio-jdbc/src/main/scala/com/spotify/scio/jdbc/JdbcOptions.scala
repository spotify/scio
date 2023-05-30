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

import org.apache.beam.sdk.io.jdbc.JdbcIO.{DefaultRetryStrategy, Read, RetryConfiguration, Write}

import java.sql.{Driver, PreparedStatement, ResultSet, SQLException}
import javax.sql.DataSource

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
  driverClass: Class[_ <: Driver]
)

object JdbcIoOptions {
  private[jdbc] val BeamDefaultBatchSize = -1L
  private[jdbc] val BeamDefaultFetchSize = -1
  private[jdbc] val BeamDefaultMaxRetryAttempts = 5
  private[jdbc] val BeamDefaultInitialRetryDelay = org.joda.time.Duration.ZERO
  private[jdbc] val BeamDefaultMaxRetryDelay = org.joda.time.Duration.ZERO
  private[jdbc] val BeamDefaultRetryConfiguration = RetryConfiguration.create(
    BeamDefaultMaxRetryAttempts,
    BeamDefaultMaxRetryDelay,
    BeamDefaultInitialRetryDelay
  )
  private[jdbc] val DefaultOutputParallelization = true
}

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
 * @param dataSourceProviderFn
 *   function to provide a custom [[javax.sql.DataSource]]
 */
final case class JdbcReadOptions[T](
  connectionOptions: JdbcConnectionOptions,
  query: String,
  statementPreparator: PreparedStatement => Unit = null,
  rowMapper: ResultSet => T,
  fetchSize: Int = JdbcIoOptions.BeamDefaultFetchSize,
  outputParallelization: Boolean = JdbcIoOptions.DefaultOutputParallelization,
  dataSourceProviderFn: () => DataSource = null,
  configOverride: Read[T] => Read[T] = null
) extends JdbcIoOptions

/**
 * Options for writing to a JDBC source.
 *
 * NB: in case of transient failures, Beam runners may execute parts of write multiple times for
 * fault tolerance. Because of that, you should avoid using INSERT statements, since that risks
 * duplicating records in the database, or failing due to primary key conflicts. Consider using
 * MERGE ("upsert") statements supported by your database instead.
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
 * @param autoSharding
 *   If true, enables using a dynamically determined number of shards to write.
 * @param dataSourceProviderFn
 *   function to provide a custom [[javax.sql.DataSource]]
 */
final case class JdbcWriteOptions[T](
  connectionOptions: JdbcConnectionOptions,
  statement: String,
  preparedStatementSetter: (T, PreparedStatement) => Unit = null,
  batchSize: Long = JdbcIoOptions.BeamDefaultBatchSize,
  retryConfiguration: RetryConfiguration = JdbcIoOptions.BeamDefaultRetryConfiguration,
  retryStrategy: SQLException => Boolean = new DefaultRetryStrategy().apply,
  autoSharding: Boolean = false,
  dataSourceProviderFn: () => DataSource = null,
  configOverride: Write[T] => Write[T] = null
) extends JdbcIoOptions
