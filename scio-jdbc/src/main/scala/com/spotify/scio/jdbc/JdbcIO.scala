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

import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TestIO}
import org.apache.beam.sdk.io.{jdbc => beam}

import java.sql.{PreparedStatement, ResultSet}

import com.spotify.scio.coders.{Coder, CoderMaterializer}

sealed trait JdbcIO[T] extends ScioIO[T]

object JdbcIO {
  final def apply[T](opts: JdbcIoOptions): JdbcIO[T] =
    new JdbcIO[T] with TestIO[T] {
      override final val tapT = EmptyTapOf[T]
      override def testId: String = s"JdbcIO(${jdbcIoId(opts)})"
    }

  private[jdbc] def jdbcIoId(opts: JdbcIoOptions): String = opts match {
    case JdbcReadOptions(connOpts, query, _, _, _) => jdbcIoId(connOpts, query)
    case JdbcWriteOptions(connOpts, statement, _, _) =>
      jdbcIoId(connOpts, statement)
  }

  private[jdbc] def jdbcIoId(opts: JdbcConnectionOptions, query: String): String = {
    val user = opts.password
      .fold(s"${opts.username}")(password => s"${opts.username}:$password")
    s"$user@${opts.connectionUrl}:$query"
  }

  private[jdbc] def dataSourceConfiguration(
    opts: JdbcConnectionOptions
  ): beam.JdbcIO.DataSourceConfiguration = {
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
}

final case class JdbcSelect[T: Coder](readOptions: JdbcReadOptions[T]) extends JdbcIO[T] {
  override type ReadP = Unit
  override type WriteP = Nothing
  override final val tapT = EmptyTapOf[T]

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(readOptions)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    var transform = beam.JdbcIO
      .read[T]()
      .withCoder(CoderMaterializer.beam(sc, Coder[T]))
      .withDataSourceConfiguration(JdbcIO.dataSourceConfiguration(readOptions.connectionOptions))
      .withQuery(readOptions.query)
      .withRowMapper(new beam.JdbcIO.RowMapper[T] {
        override def mapRow(resultSet: ResultSet): T =
          readOptions.rowMapper(resultSet)
      })
    if (readOptions.statementPreparator != null) {
      transform = transform
        .withStatementPreparator(new beam.JdbcIO.StatementPreparator {
          override def setParameters(preparedStatement: PreparedStatement): Unit =
            readOptions.statementPreparator(preparedStatement)
        })
    }
    if (readOptions.fetchSize != JdbcIoOptions.BeamDefaultFetchSize) {
      // override default fetch size.
      transform = transform.withFetchSize(readOptions.fetchSize)
    }
    sc.wrap(sc.applyInternal(transform))
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("jdbc.Select is read-only")

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

final case class JdbcWrite[T](writeOptions: JdbcWriteOptions[T]) extends JdbcIO[T] {
  override type ReadP = Nothing
  override type WriteP = Unit
  override final val tapT = EmptyTapOf[T]

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(writeOptions)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("jdbc.Write is write-only")

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    var transform = beam.JdbcIO
      .write[T]()
      .withDataSourceConfiguration(JdbcIO.dataSourceConfiguration(writeOptions.connectionOptions))
      .withStatement(writeOptions.statement)
    if (writeOptions.preparedStatementSetter != null) {
      transform = transform
        .withPreparedStatementSetter(new beam.JdbcIO.PreparedStatementSetter[T] {
          override def setParameters(element: T, preparedStatement: PreparedStatement): Unit =
            writeOptions.preparedStatementSetter(element, preparedStatement)
        })
    }
    if (writeOptions.batchSize != JdbcIoOptions.BeamDefaultBatchSize) {
      // override default batch size.
      transform = transform.withBatchSize(writeOptions.batchSize)
    }
    data.applyInternal(transform)
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}
