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

package com.spotify.scio.jdbc.sharded

import java.sql.ResultSet
import com.spotify.scio.jdbc.JdbcConnectionOptions

/**
 * A bag of options for the JDBC sharded read.
 *
 * @param connectionOptions
 *   Connection options
 * @param tableName
 *   Name of a table or materialized view to read from
 * @param shardColumn
 *   Column to shard by. Should ideally have evenly distributed values. Column type must have a
 *   corresponding [[com.spotify.scio.jdbc.sharded.Shard]] implementation.
 * @param rowMapper
 *   Function to map from a SQL [[java.sql.ResultSet]] to `T`
 * @param fetchSize
 *   Amount of rows fetched per [[java.sql.ResultSet]]. Default value is 100000. To apply an
 *   unbounded fetch size set this parameter to -1
 * @param numShards
 *   Number of shards to split the table into for reading. There is no guarantee that Beam will
 *   actually execute reads in parallel. It is up to Beam auto scaler to decide the level of
 *   parallelism to use (number of workers and threads per worker). But the behavior could be
 *   controlled with maxNumWorkers and numberOfWorkerHarnessThreads parameters (see more details
 *   about these parameters here). Defaults to 4
 * @param shard
 *   An implementation of the [[com.spotify.scio.jdbc.sharded.Shard]] trait which knows how to shard
 *   a column of a type S. Example of sharding by a column of type Long:
 *   {{{
 *               sc.jdbcShardedSelect(getShardedReadOptions(opts), Shard.range[Long])
 *   }}}
 */
final case class JdbcShardedReadOptions[T, S](
  connectionOptions: JdbcConnectionOptions,
  tableName: String,
  shardColumn: String,
  shard: Shard[S],
  rowMapper: ResultSet => T,
  fetchSize: Int = JdbcShardedReadOptions.DefaultFetchSize,
  numShards: Int = JdbcShardedReadOptions.DefaultNumShards
)

object JdbcShardedReadOptions {

  val DefaultFetchSize: Int = 100000
  val UnboundedFetchSize: Int = -1
  val DefaultNumShards: Int = 4

}
