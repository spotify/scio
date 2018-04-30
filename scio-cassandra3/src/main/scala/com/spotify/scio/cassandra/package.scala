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

import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection

import scala.concurrent.Future

/**
 * Main package for Cassandra APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.cassandra._
 * }}}
 */
package object cassandra {

  case class CassandraOptions(keyspace: String, table: String, cql: String,
                              seedNodeHost: String, seedNodePort: Int = -1,
                              username: String = null, password: String = null)

  case class CassandraIO[T](uniqueId: String) extends TestIO[T](uniqueId)

  object CassandraIO {
    def apply[T](opts: CassandraOptions): CassandraIO[T] = {
      val sb = new StringBuilder
      if (opts.username != null && opts.password != null) {
        sb.append(s"${opts.username}:${opts.password}@")
      }
      sb.append(opts.seedNodeHost)
      if (opts.seedNodePort >= 0) {
        sb.append(opts.seedNodePort)
      }
      sb.append(s"/${opts.keyspace}/${opts.table}/${opts.cql}")
      CassandraIO[T](sb.toString())
    }
  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Cassandra
   * methods.
   */
  implicit class CassandraSCollection[T](@transient val self: SCollection[T])
    extends Serializable {
    /**
     * Save this SCollection as a Cassandra table.
     *
     * Cassandra `org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter` is used to perform bulk
     * writes for better throughput. The [[com.spotify.scio.values.SCollection SCollection]] is
     * grouped by the table partition key before written to the cluster. Therefore writes only
     * occur at the end of each window in streaming mode. The bulk writer writes to all nodes in a
     * cluster so remote nodes in a multi-datacenter cluster may become a bottleneck.
     *
     * '''NOTE: this module is optimized for throughput in batch mode and not recommended for
     * * streaming mode.'''
     *
     * @param opts Cassandra options
     * @param parallelism number of concurrent bulk writers, default to number of Cassandra nodes
     * @param f function to convert input data to values for the CQL statement
     */
    def saveAsCassandra(opts: CassandraOptions, parallelism: Int = 0)
                       (f: T => Seq[Any]): Future[Tap[T]] =
      self.write(nio.Cassandra[T](opts, parallelism)(f))(())
    }
}
