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
import org.apache.beam.sdk.io.jdbc.JdbcIO
import org.apache.beam.sdk.io.jdbc.JdbcIO._

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

  val TEST_READ_TABLE_NAME = "table_read"
  val TEST_WRITE_TABLE_NAME = "table_write"

  /**
   * Options require to create a connection with remote database.
   *
   * @param username database login username
   * @param password database login password
   * @param connectionUrl connection url i.e "jdbc:mysql://[host]:[port]/db?"
   * @param driverClass subclass of java.sql.Driver
   */
  case class DbConnectionOptions(username: String,
                                 password: String,
                                 connectionUrl: String,
                                 driverClass: Class[_ <: Driver])

  /**
   * Values need to initiate read connection to database and Convert it to given type.
   *
   * @param dbConnectionOptions Options need to create a database connection.
   * @param query JDBC query string
   * @param statementPreparator
   * @param rowMapper sql Row mapper to read from ResultSet
   * @tparam T serializable type
   */
  case class JdbcReadOptions[T](dbConnectionOptions: DbConnectionOptions,
                                query: String,
                                statementPreparator: (PreparedStatement) => Unit,
                                rowMapper: (ResultSet) => T)

  /**
   * Values need to initiate write connection to database.
   *
   * @param dbConnectionOptions Options need to create a database connection.
   * @param statement JDBC query statement
   * @param preparedStatementSetter
   * @tparam T serializable type
   */
  case class JdbcWriteOptions[T](dbConnectionOptions: DbConnectionOptions,
                                 statement: String,
                                 preparedStatementSetter: (T, PreparedStatement) => Unit)


  case class JdbcTestIO[T](table: String) extends TestIO[T](table)

  /** Enhanced version of [[ScioContext]] with Jdbc and Cloud SQL methods. */
  implicit class JdbcScioContext(@transient val self: ScioContext) extends Serializable {

    /** Get an SCollection for JDBC query */
    def jdbcSelect[T: ClassTag](readOptions: JdbcReadOptions[T])
    : SCollection[T] = self.requireNotClosed {
      if (self.isTest) {
        self.getTestInput(JdbcTestIO[T](TEST_READ_TABLE_NAME))
      } else {
        val coder = self.pipeline.getCoderRegistry.getScalaCoder[T]
        val conOpt = readOptions.dbConnectionOptions
        val transformer = JdbcIO.read[T]()
          .withCoder(coder)
          .withDataSourceConfiguration(DataSourceConfiguration
            .create(conOpt.driverClass.getCanonicalName, conOpt.connectionUrl)
            .withUsername(conOpt.username)
            .withPassword(conOpt.password))
          .withQuery(readOptions.query)
          .withStatementPrepator(new StatementPreparator {
            override def setParameters(preparedStatement: PreparedStatement): Unit = {
              readOptions.statementPreparator(preparedStatement)
            }
          })
          .withRowMapper(new RowMapper[T] {
            override def mapRow(resultSet: ResultSet): T = {
              readOptions.rowMapper(resultSet)
            }
          })
        self.wrap(self.applyInternal(transformer)).setName(self.tfName)
      }
    }
  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JDBC methods */
  implicit class JdbcSCollection[T](val self: SCollection[T]) {

    /**
     * Save this SCollection as a JDBC database entry.
     *
     * @param writeOptions option to create a JDBC connection with database.
     * @return Future tap with given type.
     */
    def saveAsJdbc(writeOptions: JdbcWriteOptions[T]): Future[Tap[T]] = {
      if (self.context.isTest) {
        self.context.testOut(JdbcTestIO[T](TEST_WRITE_TABLE_NAME))(self)
      } else {
        val conOpt = writeOptions.dbConnectionOptions
        val transform = JdbcIO.write[T]()
          .withDataSourceConfiguration(DataSourceConfiguration
            .create(conOpt.driverClass.getCanonicalName, conOpt.connectionUrl)
            .withUsername(conOpt.username).withPassword(conOpt.password))
          .withStatement(writeOptions.statement)
          .withPreparedStatementSetter(new PreparedStatementSetter[T] {
            override def setParameters(element: T, preparedStatement: PreparedStatement): Unit = {
              writeOptions.preparedStatementSetter(element, preparedStatement)
            }
          })
        self.applyInternal(transform)
      }
      Future.failed(new NotImplementedError("JDBC future is not implemented"))
    }
  }

}
