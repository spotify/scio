/*
 * Copyright 2017 Spotify AB.
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

import com.spotify.scio.Implicits._
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{ScioIO, Tap, TestIO}
import org.apache.beam.sdk.io.{jdbc => beam}

import scala.concurrent.Future
import scala.reflect.ClassTag
import java.sql.{PreparedStatement, ResultSet}

sealed trait JdbcIO[T] extends TestIO[T]

object JdbcIO {
  def apply[T](opts: JdbcIoOptions): JdbcIO[T] = new JdbcIO[T] with TestIO[T] {
    override def testId: String = s"JdbcIO(${jdbcIoId(opts)})"
  }

  private[jdbc] def jdbcIoId(opts: JdbcIoOptions): String = opts match {
    case JdbcReadOptions(connOpts, query, _, _, _) => jdbcIoId(connOpts, query)
    case JdbcWriteOptions(connOpts, statement, _, _) => jdbcIoId(connOpts, statement)
  }

  private[jdbc] def jdbcIoId(opts: JdbcConnectionOptions, query: String): String = {
    val user = opts.password
      .fold(s"${opts.username}")(password => s"${opts.username}:$password")
    s"$user@${opts.connectionUrl}:$query"
  }
}

final case class JdbcSelect[T: ClassTag](readOptions: JdbcReadOptions[T]) extends ScioIO[T] {

  override type ReadP = Unit
  override type WriteP = Nothing

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(readOptions)})"

  override def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = sc.pipeline.getCoderRegistry.getScalaCoder[T](sc.options)
    var transform = beam.JdbcIO.read[T]()
      .withCoder(coder)
      .withDataSourceConfiguration(getDataSourceConfig(readOptions.connectionOptions))
      .withQuery(readOptions.query)
      .withRowMapper(new beam.JdbcIO.RowMapper[T] {
        override def mapRow(resultSet: ResultSet): T = {
          readOptions.rowMapper(resultSet)
        }
      })
    if (readOptions.statementPreparator != null) {
      transform = transform
        .withStatementPreparator(new beam.JdbcIO.StatementPreparator {
          override def setParameters(preparedStatement: PreparedStatement): Unit = {
            readOptions.statementPreparator(preparedStatement)
          }
        })
    }
    if (readOptions.fetchSize != USE_BEAM_DEFAULT_FETCH_SIZE) {
      // override default fetch size.
      transform = transform.withFetchSize(readOptions.fetchSize)
    }
    sc.wrap(sc.applyInternal(transform)).setName(sc.tfName)
  }

  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] =
    throw new IllegalStateException("jdbc.Select is read-only")

  override def tap(params: ReadP): Tap[T] =
    throw new NotImplementedError("JDBC tap is not implemented")
}

final case class JdbcWrite[T](writeOptions: JdbcWriteOptions[T]) extends ScioIO[T] {

  override type ReadP = Nothing
  override type WriteP = Unit

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(writeOptions)})"

  override def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new IllegalStateException("jdbc.Write is write-only")

  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
    val connOpts = writeOptions.connectionOptions
    var transform = beam.JdbcIO.write[T]()
      .withDataSourceConfiguration(getDataSourceConfig(writeOptions.connectionOptions))
      .withStatement(writeOptions.statement)
    if (writeOptions.preparedStatementSetter != null) {
      transform = transform
        .withPreparedStatementSetter(new beam.JdbcIO.PreparedStatementSetter[T] {
          override def setParameters(element: T, preparedStatement: PreparedStatement): Unit = {
            writeOptions.preparedStatementSetter(element, preparedStatement)
          }
        })
    }
    if(writeOptions.batchSize != USE_BEAM_DEFAULT_BATCH_SIZE) {
      // override default batch size.
      transform = transform.withBatchSize(writeOptions.batchSize)
    }
    data.applyInternal(transform)
    Future.failed(new NotImplementedError("JDBC future is not implemented"))
  }

  override def tap(params: ReadP): Tap[T] =
    throw new NotImplementedError("JDBC tap is not implemented")
}
