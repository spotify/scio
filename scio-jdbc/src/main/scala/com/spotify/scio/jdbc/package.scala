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
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.{jdbc => beam}

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

  private[jdbc] val USE_BEAM_DEFAULT_BATCH_SIZE = -1L
  private[jdbc] val USE_BEAM_DEFAULT_FETCH_SIZE = -1

  /**
    * Options for a JDBC connection.
    *
    * @param username      database login username
    * @param password      database login password if exists
    * @param connectionUrl connection url, i.e "jdbc:mysql://[host]:[port]/db?"
    * @param driverClass   subclass of [[java.sql.Driver]]
    */
  final case class JdbcConnectionOptions(username: String,
                                         password: Option[String],
                                         connectionUrl: String,
                                         driverClass: Class[_ <: Driver])

  sealed trait JdbcIoOptions

  /**
    * Options for reading from a JDBC source.
    *
    * @param connectionOptions   connection options
    * @param query               query string
    * @param statementPreparator function to prepare a [[java.sql.PreparedStatement]]
    * @param rowMapper           function to map from a SQL [[java.sql.ResultSet]] to `T`
    * @param fetchSize           use apache beam default fetch size if the value is -1
    */
  final case class JdbcReadOptions[T](connectionOptions: JdbcConnectionOptions,
                                      query: String,
                                      statementPreparator: PreparedStatement => Unit = null,
                                      rowMapper: ResultSet => T,
                                      fetchSize: Int = USE_BEAM_DEFAULT_FETCH_SIZE)
    extends JdbcIoOptions

  /**
    * Options for writing to a JDBC source.
    *
    * @param connectionOptions       connection options
    * @param statement               query statement
    * @param preparedStatementSetter function to set values in a [[java.sql.PreparedStatement]]
    * @param batchSize               use apache beam default batch size if the value is -1
    */
  final case class JdbcWriteOptions[T](
    connectionOptions: JdbcConnectionOptions,
    statement: String,
    preparedStatementSetter: (T, PreparedStatement) => Unit = null,
    batchSize: Long = USE_BEAM_DEFAULT_BATCH_SIZE)
    extends JdbcIoOptions

  private[jdbc] def getDataSourceConfig(opts: jdbc.JdbcConnectionOptions)
  : beam.JdbcIO.DataSourceConfiguration = {
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

  /** Enhanced version of [[ScioContext]] with JDBC methods. */
  implicit class JdbcScioContext(@transient val self: ScioContext) extends Serializable {
    /** Get an SCollection for a JDBC query. */
    def jdbcSelect[T: ClassTag](readOptions: JdbcReadOptions[T]): SCollection[T] =
      self.read(JdbcSelect(readOptions))
  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JDBC methods. */
  implicit class JdbcSCollection[T](val self: SCollection[T]) {
    /** Save this SCollection as a JDBC database. */
    def saveAsJdbc(writeOptions: JdbcWriteOptions[T]): Future[Tap[T]] =
      self.write(JdbcWrite(writeOptions))
  }

}

