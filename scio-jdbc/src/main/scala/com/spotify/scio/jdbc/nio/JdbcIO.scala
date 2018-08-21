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

package com.spotify.scio.jdbc.nio

import com.spotify.scio.jdbc._
import com.spotify.scio.Implicits._

import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.io.Tap
import org.apache.beam.sdk.io.{jdbc => jio}
import scala.concurrent.Future
import scala.reflect.ClassTag
import java.sql.{PreparedStatement, ResultSet}

trait JdbcIO[T] extends ScioIO[T] {
  override def toString: String = s"JdbcIO($id)"
}

object JdbcIO {
  def apply[T](opts: JdbcIoOptions): JdbcIO[T] = new JdbcIO[T] {
    override type ReadP = Nothing
    override type WriteP = Nothing
    override def read(sc: ScioContext, params: ReadP): SCollection[T] = ???
    override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = ???
    override def tap(read: Nothing): Tap[T] = ???
    override def id: String = jdbcIoId(opts)
  }
}

final case class Select[T: ClassTag](readOptions: JdbcReadOptions[T])
  extends JdbcIO[T] {

  type ReadP = Unit
  type WriteP = Nothing

  private[jdbc] def jdbcIoId(opts: JdbcConnectionOptions, query: String): String = {
    val user = opts.password
      .fold(s"${opts.username}")(password => s"${opts.username}:$password")
    s"$user@${opts.connectionUrl}:$query"
  }

  def id: String = readOptions match {
    case JdbcReadOptions(connOpts, query, _, _, _) => jdbcIoId(connOpts, query)
  }

  def read(sc: ScioContext, params: ReadP): SCollection[T] = sc.requireNotClosed {
    val coder = sc.pipeline.getCoderRegistry.getScalaCoder[T](sc.options)
    val connOpts = readOptions.connectionOptions
    var transform = jio.JdbcIO.read[T]()
      .withCoder(coder)
      .withDataSourceConfiguration(getDataSourceConfig(readOptions.connectionOptions))
      .withQuery(readOptions.query)
      .withRowMapper(new jio.JdbcIO.RowMapper[T] {
        override def mapRow(resultSet: ResultSet): T = {
          readOptions.rowMapper(resultSet)
        }
      })
    if (readOptions.statementPreparator != null) {
      transform = transform
        .withStatementPreparator(new jio.JdbcIO.StatementPreparator {
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

  def tap(read: ReadP): Tap[T] =
    throw new NotImplementedError("JDBC Tap is not implemented")

  def write(sc: SCollection[T], params: WriteP): Future[Tap[T]] = {
    throw new NotImplementedError("Can't write to a SQL Select")
  }
}

final case class Write[T](writeOptions: JdbcWriteOptions[T])
  extends JdbcIO[T] {
    type ReadP = Nothing
    type WriteP = Unit

    private[jdbc] def jdbcIoId(opts: JdbcConnectionOptions, query: String): String = {
      val user = opts.password
        .fold(s"${opts.username}")(password => s"${opts.username}:${password}")
      s"$user@${opts.connectionUrl}:$query"
    }

    def id: String = writeOptions match {
      case JdbcWriteOptions(connOpts, statement, _, _) => jdbcIoId(connOpts, statement)
    }

    def read(sc: ScioContext, params: ReadP): SCollection[T] =
      throw new NotImplementedError("Can't read from a JDBC Write")

    def tap(read: ReadP): Tap[T] =
      throw new NotImplementedError("JDBC Tap is not implemented")

    def write(sc: SCollection[T], params: WriteP): Future[Tap[T]] = {
      val connOpts = writeOptions.connectionOptions
      var transform = jio.JdbcIO.write[T]()
        .withDataSourceConfiguration(getDataSourceConfig(writeOptions.connectionOptions))
        .withStatement(writeOptions.statement)
      if (writeOptions.preparedStatementSetter != null) {
        transform = transform
          .withPreparedStatementSetter(new jio.JdbcIO.PreparedStatementSetter[T] {
            override def setParameters(element: T, preparedStatement: PreparedStatement): Unit = {
              writeOptions.preparedStatementSetter(element, preparedStatement)
            }
          })
      }
      if(writeOptions.batchSize != USE_BEAM_DEFAULT_BATCH_SIZE) {
        // override default batch size.
        transform = transform.withBatchSize(writeOptions.batchSize)
      }
      sc.applyInternal(transform)
      Future.failed(new NotImplementedError("JDBC future is not implemented"))
    }
}
