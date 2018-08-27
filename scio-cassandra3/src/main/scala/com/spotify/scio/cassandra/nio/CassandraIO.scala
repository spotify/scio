/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.cassandra.nio

import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.cassandra.{CassandraOptions, BulkOperations}
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.io.Tap
import scala.concurrent.Future

final case class CassandraIO[T](opts: CassandraOptions, parallelism: Int = 0)(f: T => Seq[Any])
  extends ScioIO[T] {

  override type ReadP = Nothing
  override type WriteP = Unit

  override def id: String = {
    val sb = new StringBuilder
    if (opts.username != null && opts.password != null) {
      sb.append(s"${opts.username}:${opts.password}@")
    }
    sb.append(opts.seedNodeHost)
    if (opts.seedNodePort >= 0) {
      sb.append(opts.seedNodePort)
    }
    sb.append(s"/${opts.keyspace}/${opts.table}/${opts.cql}")
    sb.toString()
  }

  override def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new IllegalStateException("Can't read from Cassandra")

  /**
   * Save this SCollection as a Cassandra table.
   *
   * Cassandra `org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter` is used to perform bulk
   * writes for better throughput. The [[com.spotify.scio.values.SCollection SCollection]] is
   * grouped by the table partition key before written to the cluster. Therefore writes only
   * occur at the end of each window in streaming mode. The bulk writer writes to all nodes in a
   * cluster so remote nodes in a multi-datacenter cluster may become a bottleneck.
   */
  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
    val bulkOps = new BulkOperations(opts, parallelism)
    data
      .map(f.andThen(bulkOps.serializeFn))
      .groupBy(bulkOps.partitionFn)
      .map(bulkOps.writeFn)
    Future.failed(new NotImplementedError("Cassandra future is not implemented"))
  }

  override def tap(params: ReadP): Tap[T] =
    throw new NotImplementedError("Can't read from Cassandra")
}
