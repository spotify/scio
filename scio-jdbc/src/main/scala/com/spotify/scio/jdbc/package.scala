package com.spotify.scio

import com.spotify.scio.io.Tap
import com.spotify.scio.testing.{CloudSqlIO, JdbcIO}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.{jdbc => jio}
import org.apache.beam.sdk.io.jdbc.JdbcIO.{PreparedStatementSetter, RowMapper, StatementPreparator}

import scala.concurrent.Future
import scala.reflect.ClassTag
import Implicits._

package object jdbc {

  val TEST_READ_TABLE_NAME = "table_read"
  val TEST_WRITE_TABLE_NAME = "table_write"

  /**
    * Options require to create a connection with remote database.
    * @param username database username
    * @param password database password
    * @param connectionUrl connection url i.e "jdbc:mysql://[host]:[port]/db?"
    * @param driverClassName database driver class name
    */
  case class DbConnectionOptions(username: String,
                                 password: String,
                                 connectionUrl: String,
                                 driverClassName: String)

  /**
    * Values need to initiate read connection to database and Convert it to given type.
    * @param dbConnectionOptions Options need to create a database connection.
    * @param query JDBC query string
    * @param statementPreparator
    * @param rowMapper sql Row mapper to read from ResultSet
    * @tparam T serializable type
    */
  case class JdbcReadOptions[T](dbConnectionOptions: DbConnectionOptions,
                                query: String,
                                statementPreparator: StatementPreparator,
                                rowMapper: RowMapper[T])

  /**
    * Values need to initiate write connection to database.
    * @param dbConnectionOptions Options need to create a database connection.
    * @param statement JDBC query statement
    * @param preparedStatementSetter
    * @tparam T serializable type
    */
  case class JdbcWriteOptions[T](dbConnectionOptions: DbConnectionOptions,
                                 statement: String,
                                 preparedStatementSetter: PreparedStatementSetter[T])

  /** Enhanced version of [[ScioContext]] with Jdbc and Cloud SQL methods. */
  implicit class JdbcScioContext(val self: ScioContext) {
    /**
      * Get an SCollection for a CloudSql query
      * @group input
      */
    def cloudSqlSelect[T: ClassTag](readOptions: JdbcReadOptions[T])
    : SCollection[T] = self.requireNotClosed {
      if (self.isTest) {
        self.getTestInput(CloudSqlIO[T](TEST_READ_TABLE_NAME))
      } else {
        jdbcSelect[T](readOptions)
      }
    }

    /**
      * Get as SCollection for JDBC query
      * @group input
      */
    def jdbcSelect[T: ClassTag](readOptions: JdbcReadOptions[T])
    : SCollection[T] = self.requireNotClosed {
      if(self.isTest) {
        self.getTestInput(JdbcIO[T](TEST_READ_TABLE_NAME))
      } else  {
        val coder = self.pipeline.getCoderRegistry.getScalaCoder[T]
        val conOpt = readOptions.dbConnectionOptions
        val transformer = jio.JdbcIO.read[T]()
          .withCoder(coder)
          .withDataSourceConfiguration(jio.JdbcIO.DataSourceConfiguration
            .create(conOpt.driverClassName, conOpt.connectionUrl)
            .withUsername(conOpt.username)
            .withPassword(conOpt.password))
          .withQuery(readOptions.query)
          .withStatementPrepator(readOptions.statementPreparator)
          .withRowMapper(readOptions.rowMapper)

        self.wrap(self.applyInternal(transformer)).setName(self.tfName)
      }
    }
  }

  /**
    * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JDBC and
    * Cloud SQL methods.
    */
  implicit class JdbcSCollection[T](val self: SCollection[T]) extends AnyVal{

    /**
      * Save this SCollection as a Cloud SQL database entry.
      * @param writeOptions option to create a Jdbc connection with database.
      * @return Future tap with given type.
      */
    def saveAsCloudSql(writeOptions: JdbcWriteOptions[T]): Future[Tap[T]] = {
      if (self.context.isTest) {
        self.context.testOut(CloudSqlIO[T](TEST_WRITE_TABLE_NAME))(self)
      } else {
        saveAsJdbc(writeOptions)
      }
      Future.failed(new NotImplementedError("Cloud Sql future not implemented"))
    }

    /**
      * Save this SCollection as a Jdbc database entry.
      * @param writeOptions option to create a Jdbc connection with database.
      * @return Future tap with given type.
      */
    def saveAsJdbc(writeOptions: JdbcWriteOptions[T]): Future[Tap[T]] = {
      if(self.context.isTest) {
        self.context.testOut(JdbcIO[T](TEST_WRITE_TABLE_NAME))(self)
      } else {
        val conOpt = writeOptions.dbConnectionOptions
        this.asInstanceOf[SCollection[T]].applyInternal(
          jio.JdbcIO.write[T]()
            .withDataSourceConfiguration(jio.JdbcIO.DataSourceConfiguration
              .create(conOpt.driverClassName, conOpt.connectionUrl)
              .withUsername(conOpt.username).withPassword(conOpt.password))
            .withStatement(writeOptions.statement)
            .withPreparedStatementSetter(writeOptions.preparedStatementSetter))
      }
      Future.failed(new NotImplementedError("JDBC future is not implemented"))
    }
  }
}
