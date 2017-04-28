package com.spotify.scio.examples.extra

import java.sql.{PreparedStatement, ResultSet}

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.jdbc._
import org.apache.beam.examples.common.ExampleCloudSqlOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.values.KV

// Read from cloud sql database table and write to different table in the same database.
object CloudSqlExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Ensure this has been called with required database connection details.
    val cloudSqlOptions: ExampleCloudSqlOptions = PipelineOptionsFactory.fromArgs(cmdlineArgs: _*)
      .withValidation()
      .as(classOf[ExampleCloudSqlOptions])

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val connOpt = getDbConnectionOptions(cloudSqlOptions)
    val readOptions = getReadOptions(connOpt)
    val writeOptions = getWriteOptions(connOpt)

    sc.withName("Read from Cloud Sql").cloudSqlSelect(readOptions)
      .map(kv => KV.of(kv.getKey + "_", kv.getValue)) // append _ to words
      .withName("Write to Cloud Sql").saveAsCloudSql(writeOptions)
    sc.close().waitUntilFinish()
  }

  def getJdbcUrl(options: ExampleCloudSqlOptions): String = {
    // socketFactory:com.google.cloud.sql.mysql.SocketFactory enable to create a secure connection
    // with cloud sql instance using Cloud SDK credential. With this option you don't need to
    // white list your ip to get access to database.
    // for more details look: https://cloud.google.com/sql/docs/mysql/connect-external-app#java
    // for other options look: https://cloud.google.com/sql/docs/mysql/external-connection-methods
    // look: https://github.com/GoogleCloudPlatform/cloud-sql-mysql-socket-factory for socketFactory
    s"jdbc:mysql://google/${options.getDatabase}?" +
      s"cloudSqlInstance=${options.getInstanceConnectionName}&" +
      s"socketFactory=com.google.cloud.sql.mysql.SocketFactory"
  }

  def getDbConnectionOptions(cloudSqlOptions: ExampleCloudSqlOptions): DbConnectionOptions = {
    // Basic connection details
    DbConnectionOptions(
      username = "root",
      password = "bqstats",
      driverClass = classOf[com.mysql.jdbc.Driver],
      connectionUrl = getJdbcUrl(cloudSqlOptions))
  }

  def getReadOptions(connOpts: DbConnectionOptions): JdbcReadOptions[KV[String, Long]] = {
    // Read from a table called `word_count` which has two columns 'word' and 'count'
    JdbcReadOptions(
      dbConnectionOptions = connOpts,
      query = "SELECT * FROM word_count",
      statementPreparator = (preparedStatement: PreparedStatement) => {},
      rowMapper = (resultSet: ResultSet) => {
        KV.of(resultSet.getString(1), resultSet.getLong(2))
      }
    )
  }

  def getWriteOptions(connOpt: DbConnectionOptions): JdbcWriteOptions[KV[String, Long]] = {
    // Write to a table called `result_word_count` which has two columns 'word' and 'count'
    JdbcWriteOptions(
      dbConnectionOptions = connOpt,
      statement = "INSERT INTO result_word_count values(?, ?)",
      preparedStatementSetter = (kv: KV[String, Long], preparedStatement: PreparedStatement) => {
        preparedStatement.setString(1, kv.getKey)
        preparedStatement.setLong(2, kv.getValue)
      }
    )
  }

}
