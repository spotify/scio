/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.jdbc.syntax

import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder

import scala.reflect.ClassTag
import com.spotify.scio.ScioContext
import com.spotify.scio.jdbc.sharded.{JdbcShardedReadOptions, JdbcShardedSelect}
import com.spotify.scio.jdbc.{JdbcReadOptions, JdbcSelect}

/** Enhanced version of [[ScioContext]] with JDBC methods. */
final class JdbcScioContextOps(private val self: ScioContext) extends AnyVal {

  /** Get an SCollection for a JDBC query. */
  def jdbcSelect[T: ClassTag: Coder](readOptions: JdbcReadOptions[T]): SCollection[T] =
    self.read(JdbcSelect(readOptions))

  /**
   * Sharded JDBC read from a table or materialized view.
   * @param readOptions The following paramters in the options class could be specified:
   *
   *                    shardColumn: the column to shard by. Must be
   *                    of integer/long type ideally with evenly distributed values.
   *
   *                    numShards: number of shards to split the table into for reading.
   *                    There is no guarantee that Beam will actually execute reads in parallel.
   *                    It is up to Beam auto scaler to decide the level of parallelism to use
   *                    (number of workers and threads per worker). But the behavior could be
   *                    controlled with maxNumWorkers and numberOfWorkerHarnessThreads parameters
   *                    (see more details about these parameters here). Defaults to 4.
   *
   *                    tableName: name of a table or materialized view to read from
   *
   *                    fetchSize: number of records to read from the
   *                    JDBC source per one call to a database. Default value is 100,000. Set to
   *                    -1 to make it unbounded.
   *                    shard: An implementation of the [[com.spotify.scio.jdbc.sharded.Shard]]
   *                    trait which knows how to shard a column of a type S. Example of sharding
   *                    by a column of type Long:
   *                    {{{
   *                      sc.jdbcShardedSelect(getShardedReadOptions(opts), Shard.range[Long])
   *                    }}}
   */
  def jdbcShardedSelect[T: Coder, S](
    readOptions: JdbcShardedReadOptions[T, S]
  ): SCollection[T] = self.read(JdbcShardedSelect(readOptions))

}
trait ScioContextSyntax {
  implicit def jdbcScioContextOps(sc: ScioContext): JdbcScioContextOps = new JdbcScioContextOps(sc)
}
