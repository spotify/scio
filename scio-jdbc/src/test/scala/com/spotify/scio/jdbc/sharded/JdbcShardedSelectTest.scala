/*
 * Copyright 2020 Spotify AB.
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
package sharded

import java.sql.ResultSet

import com.spotify.scio._
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing._

object JdbcShardedSelectJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    sc.jdbcShardedSelect(getShardedReadOptions(opts))
      .map(_ + "J")
      .saveAsTextFile("output")
    sc.run()
    ()
  }

  def getShardedReadOptions(opts: CloudSqlOptions): JdbcShardedReadOptions[String, Long] =
    JdbcShardedReadOptions(
      connectionOptions = JdbcJob.getConnectionOptions(opts),
      tableName = "test_table",
      shard = Shard.range[Long],
      rowMapper = (rs: ResultSet) => rs.getString("id"),
      fetchSize = 100000,
      numShards = 8,
      shardColumn = "id"
    )
}

class JdbcShardedSelectTest extends PipelineSpec {

  it should "pass correct sharded JDBC read" in {
    val args = Seq(
      "--cloudSqlUsername=john",
      "--cloudSqlPassword=secret",
      "--cloudSqlDb=mydb",
      "--cloudSqlInstanceConnectionName=project-id:zone:db-instance-name"
    )
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args.toArray)
    val readOpts = JdbcShardedSelectJob.getShardedReadOptions(opts)

    JobTest[JdbcShardedSelectJob.type]
      .args(args: _*)
      .input(JdbcShardedSelect(readOpts), Seq("a", "b", "c"))
      .output(TextIO("output")) { coll =>
        coll should containInAnyOrder(Seq("aJ", "bJ", "cJ"))
      }
      .run()
  }

}
