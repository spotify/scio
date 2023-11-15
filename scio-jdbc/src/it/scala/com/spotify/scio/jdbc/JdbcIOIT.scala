package com.spotify.scio.jdbc

import com.spotify.scio.jdbc.sharded.ShardString.SqlServerUuidLowerString
import com.spotify.scio.jdbc.sharded.{JdbcShardedReadOptions, Shard}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.PipelineOptionsFactory

object JdbcIOIT {
  val projectId = "data-integration-test"
  val regionId = "us-central1"
  val sqlServerInstanceId = "scio-sql-server-it"
  val databaseId = "shard-it"
  val tableId = "employee"
  val username = "sqlserver"
  val password = sys.props.get("cloudsql.sqlserver.password")

  val connection = JdbcConnectionOptions(
    username,
    password,
    s"jdbc:sqlserver://localhost;" +
      "socketFactoryClass=com.google.cloud.sql.sqlserver.SocketFactory;" +
      s"socketFactoryConstructorArg=$projectId:$regionId:$sqlServerInstanceId;" +
      s"databaseName=$databaseId;" +
      "encrypt=false", // otherwise we'll have to generate certificates
    classOf[com.microsoft.sqlserver.jdbc.SQLServerDriver]
  )

  val shardColumn = "guid"
  final case class Employee(guid: String, name: String)
}
class JdbcIOIT extends PipelineSpec {
  import JdbcIOIT._

  "JdbcIO" should "shard SQL Server on gid" in {
    val readOptions = JdbcShardedReadOptions(
      connection,
      tableId,
      shardColumn,
      Shard.range[SqlServerUuidLowerString],
      rs => Employee(rs.getString(1), rs.getString(2)),
      numShards = 3
    )

    runWithRealContext(PipelineOptionsFactory.create()) { sc =>
      sc.jdbcShardedSelect(readOptions) should containInAnyOrder(
        Seq(
          Employee("2AAAAAAA-BBBB-CCCC-DDDD-1EEEEEEEEEEE", "Alice"),
          Employee("3AAAAAAA-BBBB-CCCC-DDDD-2EEEEEEEEEEE", "Bob"),
          Employee("1AAAAAAA-BBBB-CCCC-DDDD-3EEEEEEEEEEE", "Carol")
        )
      )
    }
  }

}
