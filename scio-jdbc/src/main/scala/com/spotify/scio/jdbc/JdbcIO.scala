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
      case readOpts: JdbcReadOptions[_]  => apply(readOpts.connectionOptions, readOpts.query)
      case readOpts: JdbcWriteOptions[_] => apply(readOpts.connectionOptions, readOpts.statement)
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
    private[jdbc] val BeamDefaultFetchSize = -1
    private[jdbc] val DefaultOutputParallelization = true
  }

  final case class ReadParam[T](
    rowMapper: ResultSet => T,
    statementPreparator: PreparedStatement => Unit = null,
    fetchSize: Int = ReadParam.BeamDefaultFetchSize,
    outputParallelization: Boolean = ReadParam.DefaultOutputParallelization,
    dataSourceProviderFn: () => DataSource = null,
    configOverride: BJdbcIO.Read[T] => BJdbcIO.Read[T] = identity[BJdbcIO.Read[T]] _
  )

  object WriteParam {
    private[jdbc] val BeamDefaultBatchSize = -1L
    private[jdbc] val BeamDefaultMaxRetryAttempts = 5
    private[jdbc] val BeamDefaultInitialRetryDelay = org.joda.time.Duration.ZERO
    private[jdbc] val BeamDefaultMaxRetryDelay = org.joda.time.Duration.ZERO
    private[jdbc] val BeamDefaultRetryConfiguration = BJdbcIO.RetryConfiguration.create(
      BeamDefaultMaxRetryAttempts,
      BeamDefaultMaxRetryDelay,
      BeamDefaultInitialRetryDelay
    )
    private[jdbc] val DefaultRetryStrategy: SQLException => Boolean =
      new BJdbcIO.DefaultRetryStrategy().apply
    private[jdbc] val DefaultAutoSharding: Boolean = false
  }

  final case class WriteParam[T](
    preparedStatementSetter: (T, PreparedStatement) => Unit,
    batchSize: Long = WriteParam.BeamDefaultBatchSize,
    retryConfiguration: BJdbcIO.RetryConfiguration = WriteParam.BeamDefaultRetryConfiguration,
    retryStrategy: SQLException => Boolean = WriteParam.DefaultRetryStrategy,
    autoSharding: Boolean = WriteParam.DefaultAutoSharding,
    dataSourceProviderFn: () => DataSource = null,
    configOverride: BJdbcIO.Write[T] => BJdbcIO.Write[T] = identity[BJdbcIO.Write[T]] _
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
