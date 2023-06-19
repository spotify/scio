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

package com.spotify.scio.jdbc.syntax

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.jdbc.{JdbcConnectionOptions, JdbcIO, JdbcWrite, JdbcWriteOptions}
import com.spotify.scio.jdbc.JdbcIO.WriteParam
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.jdbc.JdbcIO.{RetryConfiguration, Write}

import java.sql.{PreparedStatement, SQLException}
import javax.sql.DataSource

/** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JDBC methods. */
final class JdbcSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  /** Save this SCollection as a JDBC database. */
  @deprecated("Use another overload with multiple parameters", since = "0.13.0")
  def saveAsJdbc(writeOptions: JdbcWriteOptions[T]): ClosedTap[Nothing] =
    saveAsJdbc(
      writeOptions.connectionOptions,
      writeOptions.statement,
      writeOptions.batchSize,
      writeOptions.retryConfiguration,
      writeOptions.retryStrategy
    )(writeOptions.preparedStatementSetter)

  /**
   * Save this SCollection as a JDBC database.
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
   * @param configOverride
   *   function to override or replace a Write transform before applying it
   */
  def saveAsJdbc(
    connectionOptions: JdbcConnectionOptions,
    statement: String,
    batchSize: Long = WriteParam.BeamDefaultBatchSize,
    retryConfiguration: RetryConfiguration = WriteParam.BeamDefaultRetryConfiguration,
    retryStrategy: SQLException => Boolean = WriteParam.DefaultRetryStrategy,
    autoSharding: Boolean = WriteParam.DefaultAutoSharding,
    dataSourceProviderFn: () => DataSource = WriteParam.DefaultDataSourceProviderFn,
    configOverride: Write[T] => Write[T] = WriteParam.defaultConfigOverride[T]
  )(preparedStatementSetter: (T, PreparedStatement) => Unit): ClosedTap[Nothing] =
    self.write(JdbcWrite[T](connectionOptions, statement))(
      JdbcIO.WriteParam(
        preparedStatementSetter,
        batchSize,
        retryConfiguration,
        retryStrategy,
        autoSharding,
        dataSourceProviderFn,
        configOverride
      )
    )
}

trait SCollectionSyntax {
  implicit def jdbcSCollectionOps[T](sc: SCollection[T]): JdbcSCollectionOps[T] =
    new JdbcSCollectionOps(sc)
}
