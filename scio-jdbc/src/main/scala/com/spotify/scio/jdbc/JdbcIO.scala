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

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.jdbc.{JdbcIO => BJdbcIO}

import java.sql.{PreparedStatement, ResultSet, SQLException}
import javax.sql.DataSource

sealed trait JdbcIO[T] extends ScioIO[T]

object JdbcIO {

  @deprecated("Use new API overloads with multiple parameters", since = "0.13.0")
  final def apply[T](opts: JdbcIoOptions): JdbcIO[T] =
    opts match {
      case readOpts: JdbcReadOptions[_]   => apply(readOpts.connectionOptions, readOpts.query)
      case writeOpts: JdbcWriteOptions[_] => apply(writeOpts.connectionOptions, writeOpts.statement)
    }

  final def apply[T](opts: JdbcConnectionOptions, query: String): JdbcIO[T] =
    new JdbcIO[T] with TestIO[T] {
      final override val tapT = EmptyTapOf[T]
      override def testId: String = s"JdbcIO(${jdbcIoId(opts, query)})"
    }

  private[jdbc] def jdbcIoId(opts: JdbcConnectionOptions, query: String): String = {
    val user = opts.password
      .fold(s"${opts.username}")(password => s"${opts.username}:$password")
    s"$user@${opts.connectionUrl}:$query"
  }

  private[jdbc] def dataSourceConfiguration(
    opts: JdbcConnectionOptions
  ): BJdbcIO.DataSourceConfiguration =
    opts.password match {
      case Some(pass) =>
        BJdbcIO.DataSourceConfiguration
          .create(opts.driverClass.getCanonicalName, opts.connectionUrl)
          .withUsername(opts.username)
          .withPassword(pass)
      case None =>
        BJdbcIO.DataSourceConfiguration
          .create(opts.driverClass.getCanonicalName, opts.connectionUrl)
          .withUsername(opts.username)
    }

  object ReadParam {
    val BeamDefaultFetchSize: Int = -1
    val DefaultOutputParallelization: Boolean = true
    val DefaultStatementPreparator: PreparedStatement => Unit = null
    val DefaultDataSourceProviderFn: () => DataSource = null
    def dfaultConfigOverride[T]: BJdbcIO.Read[T] => BJdbcIO.Read[T] = identity
  }

  final case class ReadParam[T] private (
    rowMapper: ResultSet => T,
    statementPreparator: PreparedStatement => Unit = ReadParam.DefaultStatementPreparator,
    fetchSize: Int = ReadParam.BeamDefaultFetchSize,
    outputParallelization: Boolean = ReadParam.DefaultOutputParallelization,
    dataSourceProviderFn: () => DataSource = ReadParam.DefaultDataSourceProviderFn,
    configOverride: BJdbcIO.Read[T] => BJdbcIO.Read[T] = ReadParam.dfaultConfigOverride
  )

  object WriteParam {
    val BeamDefaultBatchSize = -1L
    val BeamDefaultMaxRetryAttempts = 5
    val BeamDefaultInitialRetryDelay = org.joda.time.Duration.ZERO
    val BeamDefaultMaxRetryDelay = org.joda.time.Duration.ZERO
    val BeamDefaultRetryConfiguration = BJdbcIO.RetryConfiguration.create(
      BeamDefaultMaxRetryAttempts,
      BeamDefaultMaxRetryDelay,
      BeamDefaultInitialRetryDelay
    )
    val DefaultRetryStrategy: SQLException => Boolean =
      new BJdbcIO.DefaultRetryStrategy().apply
    val DefaultAutoSharding: Boolean = false
    val DefaultDataSourceProviderFn: () => DataSource = null
    def defaultConfigOverride[T]: BJdbcIO.Write[T] => BJdbcIO.Write[T] = identity
  }

  final case class WriteParam[T] private (
    preparedStatementSetter: (T, PreparedStatement) => Unit,
    batchSize: Long = WriteParam.BeamDefaultBatchSize,
    retryConfiguration: BJdbcIO.RetryConfiguration = WriteParam.BeamDefaultRetryConfiguration,
    retryStrategy: SQLException => Boolean = WriteParam.DefaultRetryStrategy,
    autoSharding: Boolean = WriteParam.DefaultAutoSharding,
    dataSourceProviderFn: () => DataSource = WriteParam.DefaultDataSourceProviderFn,
    configOverride: BJdbcIO.Write[T] => BJdbcIO.Write[T] = WriteParam.defaultConfigOverride
  )
}

final case class JdbcSelect[T: Coder](opts: JdbcConnectionOptions, query: String)
    extends JdbcIO[T] {
  override type ReadP = JdbcIO.ReadParam[T]
  override type WriteP = Nothing
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(opts, query)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    var transform = BJdbcIO
      .read[T]()
      .withCoder(coder)
      .withDataSourceConfiguration(JdbcIO.dataSourceConfiguration(opts))
      .withQuery(query)
      .withRowMapper(params.rowMapper(_))
      .withOutputParallelization(params.outputParallelization)

    if (params.dataSourceProviderFn != null) {
      transform.withDataSourceProviderFn((_: Void) => params.dataSourceProviderFn())
    }
    if (params.statementPreparator != null) {
      transform = transform
        .withStatementPreparator(params.statementPreparator(_))
    }
    if (params.fetchSize != JdbcIO.ReadParam.BeamDefaultFetchSize) {
      // override default fetch size.
      transform = transform.withFetchSize(params.fetchSize)
    }

    sc.applyTransform(params.configOverride(transform))
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("jdbc.Select is read-only")

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

final case class JdbcWrite[T](opts: JdbcConnectionOptions, statement: String) extends JdbcIO[T] {
  override type ReadP = Nothing
  override type WriteP = JdbcIO.WriteParam[T]
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(opts, statement)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("jdbc.Write is write-only")

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    var transform = BJdbcIO
      .write[T]()
      .withDataSourceConfiguration(JdbcIO.dataSourceConfiguration(opts))
      .withStatement(statement)

    if (params.dataSourceProviderFn != null) {
      transform.withDataSourceProviderFn((_: Void) => params.dataSourceProviderFn())
    }
    if (params.preparedStatementSetter != null) {
      transform = transform
        .withPreparedStatementSetter(params.preparedStatementSetter(_, _))
    }
    if (params.batchSize != JdbcIO.WriteParam.BeamDefaultBatchSize) {
      // override default batch size.
      transform = transform.withBatchSize(params.batchSize)
    }
    if (params.autoSharding) {
      transform = transform.withAutoSharding()
    }

    transform = transform
      .withRetryConfiguration(params.retryConfiguration)
      .withRetryStrategy(params.retryStrategy.apply)

    data.applyInternal(params.configOverride(transform))
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}
