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

import com.spotify.scio.Implicits._
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
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
   * @param password database login password
   * @param connectionUrl connection url, i.e "jdbc:mysql://[host]:[port]/db?"
   * @param driverClass subclass of [[java.sql.Driver]]
   */
  case class JdbcConnectionOptions(username: String,
                                   password: String,
                                   connectionUrl: String,
                                   driverClass: Class[_ <: Driver])

  sealed trait JdbcIoOptions

  /**
   * Options for reading from a JDBC source.
   *
   * @param connectionOptions connection options
   * @param query query string
   * @param statementPreparator function to prepare a [[PreparedStatement]]
   * @param rowMapper function to map from a SQL [[ResultSet]] to `T`
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
   * @param preparedStatementSetter function to set values in a [[PreparedStatement]]
   */
  case class JdbcWriteOptions[T](connectionOptions: JdbcConnectionOptions,
                                 statement: String,
                                 preparedStatementSetter: (T, PreparedStatement) => Unit = null)
    extends JdbcIoOptions

  case class JdbcIO[T](uniqueId: String) extends TestIO[T](uniqueId)

  object JdbcIO {
    def apply[T](jdbcIoOptions: JdbcIoOptions): JdbcIO[T] = JdbcIO[T](jdbcIoId(jdbcIoOptions))
  }

  private def jdbcIoId(opts: JdbcConnectionOptions, query: String): String =
    s"${opts.username}:${opts.password}@${opts.connectionUrl}:$query"

  private def jdbcIoId(opts: JdbcIoOptions): String = opts match {
    case JdbcReadOptions(connOpts, query, _, _) => jdbcIoId(connOpts, query)
    case JdbcWriteOptions(connOpts, statement, _) => jdbcIoId(connOpts, statement)
  }

  /** Enhanced version of [[ScioContext]] with JDBC methods. */
  implicit class JdbcScioContext(@transient val self: ScioContext) extends Serializable {
    /** Get an SCollection for JDBC query. */
    def jdbcSelect[T: ClassTag](readOptions: JdbcReadOptions[T])
    : SCollection[T] = self.requireNotClosed {
      if (self.isTest) {
        self.getTestInput(JdbcIO[T](readOptions))
      } else {
        val coder = self.pipeline.getCoderRegistry.getScalaCoder[T]
        val connOpts = readOptions.connectionOptions
        var transform = jio.JdbcIO.read[T]()
          .withCoder(coder)
          .withDataSourceConfiguration(jio.JdbcIO.DataSourceConfiguration
            .create(connOpts.driverClass.getCanonicalName, connOpts.connectionUrl)
            .withUsername(connOpts.username)
            .withPassword(connOpts.password))
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
        self.wrap(self.applyInternal(transform)).setName(self.tfName)
      }
    }
  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JDBC methods. */
  implicit class JdbcSCollection[T](val self: SCollection[T]) {
    /** Save this SCollection as a JDBC database entry. */
    def saveAsJdbc(writeOptions: JdbcWriteOptions[T]): Future[Tap[T]] = {
      if (self.context.isTest) {
        self.context.testOut(JdbcIO[T](writeOptions))(self)
      } else {
        val connOpts = writeOptions.connectionOptions
        var transform = jio.JdbcIO.write[T]()
          .withDataSourceConfiguration(jio.JdbcIO.DataSourceConfiguration
            .create(connOpts.driverClass.getCanonicalName, connOpts.connectionUrl)
            .withUsername(connOpts.username).withPassword(connOpts.password))
          .withStatement(writeOptions.statement)
        if (writeOptions.preparedStatementSetter != null) {
          transform = transform
            .withPreparedStatementSetter(new jio.JdbcIO.PreparedStatementSetter[T] {
              override def setParameters(element: T, preparedStatement: PreparedStatement): Unit = {
                writeOptions.preparedStatementSetter(element, preparedStatement)
              }
            })
        }
        self.applyInternal(transform)
      }
      Future.failed(new NotImplementedError("JDBC future is not implemented"))
    }
  }

}
