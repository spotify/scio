/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.jdbc

import com.spotify.scio.jdbc.sharded.ShardString.SqlServerUuidLowerString
import com.spotify.scio.jdbc.sharded.{JdbcShardedReadOptions, Shard}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.PipelineOptionsFactory

object JdbcIOIT {
  final case class Employee(guid: String, name: String)
}

// integration/runMain com.spotify.scio.jdbc.PopulateTestData to re-populate data for integration tests
class JdbcIOIT extends PipelineSpec {
  import JdbcIOIT._

  "JdbcIO" should "shard SQL Server on gid" in {
    val readOptions = JdbcShardedReadOptions(
      SqlServer.connection,
      "employee",
      "guid",
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
