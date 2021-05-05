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

import java.lang.management.ManagementFactory

import com.datastax.driver.core.{Cluster, ProtocolVersion}
import com.google.protobuf.ByteString
import org.apache.cassandra.db.marshal.CompositeType
import org.apache.cassandra.hadoop.cql3._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.TaskAttemptContext

import scala.jdk.CollectionConverters._

private[cassandra] class BulkOperations(val opts: CassandraOptions, val parallelism: Int)
    extends Serializable {
  case class BulkConfig(
    protocol: ProtocolVersion,
    partitioner: String,
    numOfNodes: Int,
    tableSchema: String,
    partitionKeyIndices: Seq[Int],
    dataTypes: Seq[DataTypeExternalizer]
  )

  private val config = {
    var b = Cluster.builder().addContactPoint(opts.seedNodeHost)
    if (opts.seedNodePort >= 0) {
      b = b.withPort(opts.seedNodePort)
    }
    if (opts.username != null && opts.password != null) {
      b = b.withCredentials(opts.username, opts.password)
    }
    val cluster = b.build()

    val table = for {
      k <-
        cluster.getMetadata.getKeyspaces.asScala
          .find(_.getName == opts.keyspace)
      t <- k.getTables.asScala.find(_.getName == opts.table)
    } yield t
    require(table.isDefined, s"Invalid keyspace.table: ${opts.keyspace}.${opts.table}")

    val protocol = CompatUtil.getProtocolVersion(cluster)
    val partitioner = cluster.getMetadata.getPartitioner
    val numOfNodes = cluster.getMetadata.getAllHosts.size()
    val tableSchema = table.get.asCQLQuery()

    val variables =
      cluster.connect().prepare(opts.cql).getVariables.asList().asScala
    val partitionKeys = table.get.getPartitionKey.asScala.map(_.getName).toSet
    val partitionKeyIndices = variables.iterator
      .map(_.getName)
      .zipWithIndex
      .filter(t => partitionKeys.contains(t._1))
      .map(_._2)
      .toArray
    val dataTypes = variables.iterator.map(v => DataTypeExternalizer(v.getType)).toSeq
    cluster.close()

    BulkConfig(protocol, partitioner, numOfNodes, tableSchema, partitionKeyIndices, dataTypes)
  }

  val serializeFn: Seq[Any] => Array[ByteString] = (values: Seq[Any]) => {
    val b = Array.newBuilder[ByteString]
    val i = values.iterator
    val j = config.dataTypes.iterator
    while (i.hasNext && j.hasNext) {
      b += ByteString.copyFrom(CompatUtil.serialize(j.next().get, i.next(), config.protocol))
    }
    b.result()
  }

  val partitionFn: Array[ByteString] => Int = {
    // Partition tokens equally across workers regardless of cluster token distribution
    // This may not create 1-to-1 mapping between partitions and C* nodes but handles multi-DC
    // clusters better
    val maxToken = BigInt(CompatUtil.maxToken(config.partitioner))
    val minToken = BigInt(CompatUtil.minToken(config.partitioner))
    val numPartitions = if (parallelism > 0) parallelism else config.numOfNodes
    val (q, mod) = (maxToken - minToken + 1) /% numPartitions
    val rangePerGroup = (if (mod != 0) q + 1 else q).bigInteger

    (values: Array[ByteString]) => {
      val key = if (config.partitionKeyIndices.length == 1) {
        values(config.partitionKeyIndices.head).asReadOnlyByteBuffer()
      } else {
        val keys = config.partitionKeyIndices.map(values).map(_.asReadOnlyByteBuffer())
        CompositeType.build(keys: _*)
      }
      val token = CompatUtil.getToken(config.partitioner, key)
      token.divide(rangePerGroup).intValue()
    }
  }

  val writeFn: ((Int, Iterable[Array[ByteString]])) => Unit =
    (kv: (Int, Iterable[Array[ByteString]])) => {
      val w = newWriter
      kv._2.foreach(row => w.write(null, row.map(_.asReadOnlyByteBuffer()).toList.asJava))
      w.close(null: TaskAttemptContext)
    }

  private def newWriter: CqlBulkRecordWriter = {
    val conf = new Configuration()
    CqlBulkRecordWriterUtil.newWriter(
      conf,
      opts.seedNodeHost,
      opts.seedNodePort,
      opts.username,
      opts.password,
      opts.keyspace,
      opts.table,
      config.partitioner,
      config.tableSchema,
      opts.cql
    )
  }
}

private[cassandra] object CassandraUtil {
  def cleanup(): Unit = {
    val mbs = ManagementFactory.getPlatformMBeanServer
    mbs
      .queryNames(null, null)
      .asScala
      .filter(_.getCanonicalName.startsWith("org.apache.cassandra."))
      .foreach(mbs.unregisterMBean)
  }
}
