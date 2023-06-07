package com.spotify.scio.cassandra

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Cassandra methods.
 */
class CassandraSCollection[T](@transient private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection as a Cassandra table.
   *
   * Cassandra `org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter` is used to perform bulk writes
   * for better throughput. The [[com.spotify.scio.values.SCollection SCollection]] is grouped by
   * the table partition key before written to the cluster. Therefore writes only occur at the end
   * of each window in streaming mode. The bulk writer writes to all nodes in a cluster so remote
   * nodes in a multi-datacenter cluster may become a bottleneck.
   *
   * '''NOTE: this module is optimized for throughput in batch mode and not recommended for
   * streaming mode.'''
   *
   * @param opts
   *   Cassandra options
   * @param parallelism
   *   number of concurrent bulk writers, default to number of Cassandra nodes
   * @param f
   *   function to convert input data to values for the CQL statement
   */
  def saveAsCassandra(
    opts: CassandraOptions,
    parallelism: Int = CassandraIO.WriteParam.DefaultPar
  )(f: T => Seq[Any]): ClosedTap[Nothing] =
    self.write(CassandraIO[T](opts))(CassandraIO.WriteParam(f, parallelism))
}
