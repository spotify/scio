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

package com.spotify.scio

import java.sql.{Driver, PreparedStatement, ResultSet}

import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration
import org.apache.beam.sdk.io.{jdbc => jio}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Main package for JDBC APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.jdbc._
 * }}}
 */
package object jdbc {

  /**
   * Options for a JDBC connection.
   *
   * @param username database login username
   * @param password database login password if exists
   * @param connectionUrl connection url, i.e "jdbc:mysql://[host]:[port]/db?"
   * @param driverClass subclass of [[java.sql.Driver]]
   */
  case class JdbcConnectionOptions(username: String,
                                   password: Option[String],
                                   connectionUrl: String,
                                   driverClass: Class[_ <: Driver])

  sealed trait JdbcIoOptions

  /**
   * Options for reading from a JDBC source.
   *
   * @param connectionOptions connection options
   * @param query query string
   * @param statementPreparator function to prepare a [[java.sql.PreparedStatement]]
   * @param rowMapper function to map from a SQL [[java.sql.ResultSet]] to `T`
   */
  case class JdbcReadOptions[T](connectionOptions: JdbcConnectionOptions,
                                query: String,
                                statementPreparator: PreparedStatement => Unit = null,
                                rowMapper: ResultSet => T) extends JdbcIoOptions

  /**
   * Options for writing to a JDBC source.
   *
   * @param connectionOptions connection options
   * @param statement query statement
   * @param preparedStatementSetter function to set values in a [[java.sql.PreparedStatement]]
   */
  case class JdbcWriteOptions[T](connectionOptions: JdbcConnectionOptions,
                                 statement: String,
                                 preparedStatementSetter: (T, PreparedStatement) => Unit = null)
    extends JdbcIoOptions

  case class JdbcIO[T](uniqueId: String) extends TestIO[T](uniqueId)

  object JdbcIO {
    def apply[T](jdbcIoOptions: JdbcIoOptions): JdbcIO[T] = JdbcIO[T](jdbcIoId(jdbcIoOptions))
  }

  private[jdbc] def jdbcIoId(opts: JdbcConnectionOptions, query: String): String = {
    val user = opts.password
      .fold(s"${opts.username}")(password => s"${opts.username}:${password}")
    s"$user@${opts.connectionUrl}:$query"
  }

  private def jdbcIoId(opts: JdbcIoOptions): String = opts match {
    case JdbcReadOptions(connOpts, query, _, _) => jdbcIoId(connOpts, query)
    case JdbcWriteOptions(connOpts, statement, _) => jdbcIoId(connOpts, statement)
  }

  private[jdbc] def getDataSourceConfig(
                                         opts: jdbc.JdbcConnectionOptions
                                       ): DataSourceConfiguration = {
    opts.password match {
      case Some(pass) => {
        jio.JdbcIO.DataSourceConfiguration
          .create(opts.driverClass.getCanonicalName, opts.connectionUrl)
          .withUsername(opts.username)
          .withPassword(pass)
      }
      case None => {
        jio.JdbcIO.DataSourceConfiguration
          .create(opts.driverClass.getCanonicalName, opts.connectionUrl)
          .withUsername(opts.username)
      }
    }
  }

  /** Enhanced version of [[ScioContext]] with JDBC methods. */
  implicit class JdbcScioContext(@transient val self: ScioContext) extends Serializable {
    /** Get an SCollection for a JDBC query. */
    def jdbcSelect[T: ClassTag](readOptions: JdbcReadOptions[T])
    : SCollection[T] =
      self.read(nio.Select(readOptions))
  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JDBC methods. */
  implicit class JdbcSCollection[T](val self: SCollection[T]) {
    /** Save this SCollection as a JDBC database. */
    def saveAsJdbc(writeOptions: JdbcWriteOptions[T]): Future[Tap[T]] =
      self.write(nio.Write(writeOptions))
  }

}

