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
import org.apache.beam.sdk.io.{jdbc => beam}

sealed trait JdbcIO[T] extends ScioIO[T]

object JdbcIO {
  final def apply[T](opts: JdbcIoOptions): JdbcIO[T] =
    new JdbcIO[T] with TestIO[T] {
      final override val tapT = EmptyTapOf[T]
      override def testId: String = s"JdbcIO(${jdbcIoId(opts)})"
    }

  private[jdbc] def jdbcIoId(opts: JdbcIoOptions): String = opts match {
    case readOpts: JdbcReadOptions[_] =>
      jdbcIoId(readOpts.connectionOptions, readOpts.query)
    case writeOpts: JdbcWriteOptions[_] =>
      jdbcIoId(writeOpts.connectionOptions, writeOpts.statement)
  }

  private[jdbc] def jdbcIoId(opts: JdbcConnectionOptions, query: String): String = {
    val user = opts.password
      .fold(s"${opts.username}")(password => s"${opts.username}:$password")
    s"$user@${opts.connectionUrl}:$query"
  }

  private[jdbc] def dataSourceConfiguration(
    opts: JdbcConnectionOptions
  ): beam.JdbcIO.DataSourceConfiguration =
    opts.password match {
      case Some(pass) =>
        beam.JdbcIO.DataSourceConfiguration
          .create(opts.driverClass.getCanonicalName, opts.connectionUrl)
          .withUsername(opts.username)
          .withPassword(pass)
      case None =>
        beam.JdbcIO.DataSourceConfiguration
          .create(opts.driverClass.getCanonicalName, opts.connectionUrl)
          .withUsername(opts.username)
    }
}

final case class JdbcSelect[T: Coder](readOptions: JdbcReadOptions[T]) extends JdbcIO[T] {
  override type ReadP = Unit
  override type WriteP = Nothing
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(readOptions)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    var transform = beam.JdbcIO
      .read[T]()
      .withCoder(coder)
      .withDataSourceConfiguration(JdbcIO.dataSourceConfiguration(readOptions.connectionOptions))
      .withQuery(readOptions.query)
      .withRowMapper(readOptions.rowMapper(_))
      .withOutputParallelization(readOptions.outputParallelization)

    if (readOptions.dataSourceProviderFn != null) {
      transform.withDataSourceProviderFn((_: Void) => readOptions.dataSourceProviderFn())
    }
    if (readOptions.statementPreparator != null) {
      transform = transform
        .withStatementPreparator(readOptions.statementPreparator(_))
    }
    if (readOptions.fetchSize != JdbcIoOptions.BeamDefaultFetchSize) {
      // override default fetch size.
      transform = transform.withFetchSize(readOptions.fetchSize)
    }
    if (readOptions.configOverride != null) {
      transform = readOptions.configOverride(transform)
    }

    sc.applyTransform(transform)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("jdbc.Select is read-only")

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

final case class JdbcWrite[T](writeOptions: JdbcWriteOptions[T]) extends JdbcIO[T] {
  override type ReadP = Nothing
  override type WriteP = Unit
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(writeOptions)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("jdbc.Write is write-only")

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    var transform = beam.JdbcIO
      .write[T]()
      .withDataSourceConfiguration(JdbcIO.dataSourceConfiguration(writeOptions.connectionOptions))
      .withStatement(writeOptions.statement)

    if (writeOptions.dataSourceProviderFn != null) {
      transform.withDataSourceProviderFn((_: Void) => writeOptions.dataSourceProviderFn())
    }
    if (writeOptions.preparedStatementSetter != null) {
      transform = transform
        .withPreparedStatementSetter(writeOptions.preparedStatementSetter(_, _))
    }
    if (writeOptions.batchSize != JdbcIoOptions.BeamDefaultBatchSize) {
      // override default batch size.
      transform = transform.withBatchSize(writeOptions.batchSize)
    }
    if (writeOptions.autoSharding) {
      transform = transform.withAutoSharding()
    }

    transform = transform
      .withRetryConfiguration(writeOptions.retryConfiguration)
      .withRetryStrategy(writeOptions.retryStrategy.apply)

    if (writeOptions.configOverride != null) {
      transform = writeOptions.configOverride(transform)
    }

    data.applyInternal(transform)
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}
