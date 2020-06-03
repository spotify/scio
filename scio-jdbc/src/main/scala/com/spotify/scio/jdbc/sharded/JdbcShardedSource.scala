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

import java.sql.Connection
import java.sql.ResultSet
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.io.BoundedSource
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.joda.time.Instant
import org.slf4j.LoggerFactory
import java.util.{List => jList}
import scala.jdk.CollectionConverters._

final private[jdbc] class JdbcShardedSource[T, S](
  private val readOptions: JdbcShardedReadOptions[T],
  coder: BCoder[T],
  shard: Shard[S],
  private val query: Option[ShardQuery] = None
) extends BoundedSource[T] {

  private val ShardBoundsQueryTemplate = "SELECT min(%s) min, max(%s) max FROM %s"

  private val log = LoggerFactory.getLogger(this.getClass)

  private def getShardColumnRange: Option[Range[S]] = {
    val connection = JdbcUtils.createConnection(readOptions.connectionOptions)
    try {
      if (
        JdbcUtils
          .getIndexedColumns(connection, readOptions.tableName)
          .find(_.equalsIgnoreCase(readOptions.shardColumn))
          .isEmpty
      ) {

        throw new UnsupportedOperationException(
          s"Shard column '${readOptions.shardColumn}' isn't indexed. Sharding would be " +
            s"inefficient'"
        )
      }

      val query = ShardBoundsQueryTemplate.format(
        readOptions.shardColumn,
        readOptions.shardColumn,
        readOptions.tableName
      )
      log.info("Executing query = [{}]", query)
      val rs = connection.createStatement.executeQuery(query)

      if (rs.next) {
        Some(
          new Range(
            shard.columnValueDecoder(rs, "min"),
            shard.columnValueDecoder(rs, "max")
          )
        )
      } else {
        log.warn("The table is empty. Nothing to read.")
        None
      }
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  override def split(
    desiredBundleSizeBytes: Long,
    options: PipelineOptions
  ): jList[_ <: BoundedSource[T]] = {
    getShardColumnRange match {
      case None =>
        List.empty.asJava
      case Some(range) =>
        shard
          .partition(range, readOptions.numShards)
          .map { query =>
            new JdbcShardedSource(readOptions, coder, shard, Some(query))
          }
          .asJava
    }
  }

  override def getEstimatedSizeBytes(options: PipelineOptions): Long =
    // Returning 0 because there is no good estimate
    0

  override def getOutputCoder: BCoder[T] = coder

  override def createReader(options: PipelineOptions): BoundedSource.BoundedReader[T] =
    query match {
      case Some(q) =>
        new JdbcShardedReader(this, q)
      case None =>
        throw new UnsupportedOperationException(
          "Not possible to create a read for a source with " +
            "the empty query"
        )
    }

  private class JdbcShardedReader(source: JdbcShardedSource[T, S], query: ShardQuery)
      extends BoundedSource.BoundedReader[T] {
    private var connection: Connection = _
    private var resultSet: ResultSet = _

    override def start(): Boolean = {
      connection = JdbcUtils.createConnection(source.readOptions.connectionOptions)
      val statement = connection.createStatement(
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY
      )

      if (source.readOptions.fetchSize != JdbcShardedReadOptions.UnboundedFetchSize) {
        log.info("Setting a user defined fetch size: [%s]".format(source.readOptions.fetchSize))
        statement.setFetchSize(source.readOptions.fetchSize)
      }

      val queryString =
        ShardQuery.toSelectStatement(query, readOptions.tableName, readOptions.shardColumn)

      log.info(s"Running a query: [$queryString]")
      resultSet = statement.executeQuery(queryString)
      resultSet.next
    }

    override def advance: Boolean = resultSet.next

    override def getCurrent: T = source.readOptions.rowMapper.apply(resultSet)

    override def getCurrentTimestamp: Instant = BoundedWindow.TIMESTAMP_MIN_VALUE

    override def close(): Unit = {
      if (connection != null) {
        connection.close()
        log.info("JDBC connection closed")
      }
    }

    override def getCurrentSource: BoundedSource[T] = source
  }

}
