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

package com.spotify.scio.cassandra

import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap}
import com.spotify.scio.io.TapT

final case class CassandraIO[T](opts: CassandraOptions) extends ScioIO[T] {
  override type ReadP = Nothing
  override type WriteP = CassandraIO.WriteParam[T]
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("Can't read from Cassandra")

  /**
   * Save this SCollection as a Cassandra table.
   *
   * Cassandra `org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter` is used to perform bulk
   * writes for better throughput. The [[com.spotify.scio.values.SCollection SCollection]] is
   * grouped by the table partition key before written to the cluster. Therefore writes only
   * occur at the end of each window in streaming mode. The bulk writer writes to all nodes in a
   * cluster so remote nodes in a multi-datacenter cluster may become a bottleneck.
   */
  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val bulkOps = new BulkOperations(opts, params.parallelism)
    data
      .map(params.outputFn.andThen(bulkOps.serializeFn))
      .groupBy(bulkOps.partitionFn)
      .map(bulkOps.writeFn)
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object CassandraIO {
  object WriteParam {
    private[cassandra] val DefaultPar = 0
  }

  final case class WriteParam[T] private[cassandra] (
    outputFn: T => Seq[Any],
    parallelism: Int = WriteParam.DefaultPar
  )
}
