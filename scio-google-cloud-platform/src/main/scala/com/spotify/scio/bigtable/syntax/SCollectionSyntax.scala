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

package com.spotify.scio.bigtable.syntax

import com.google.bigtable.v2._
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.protobuf.ByteString
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import org.joda.time.Duration
import com.spotify.scio.bigtable.{BTOptions, BigtableTypedIO, BigtableWrite}
import com.spotify.scio.coders.Coder
import magnolify.bigtable.BigtableType

/** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Bigtable methods. */
final class SCollectionMutationOps[T <: Mutation](
  private val self: SCollection[(ByteString, Iterable[T])]
) {

  /** Save this SCollection as a Bigtable table. Note that elements must be of type `Mutation`. */
  def saveAsBigtable(projectId: String, instanceId: String, tableId: String): ClosedTap[Nothing] =
    self.write(BigtableWrite[T](projectId, instanceId, tableId))(BigtableWrite.Default)

  /** Save this SCollection as a Bigtable table. Note that elements must be of type `Mutation`. */
  def saveAsBigtable(bigtableOptions: BigtableOptions, tableId: String): ClosedTap[Nothing] =
    self.write(BigtableWrite[T](bigtableOptions, tableId))(BigtableWrite.Default)

  /**
   * Save this SCollection as a Bigtable table. This version supports batching. Note that elements
   * must be of type `Mutation`.
   */
  def saveAsBigtable(
    bigtableOptions: BigtableOptions,
    tableId: String,
    numOfShards: Int,
    flushInterval: Duration = BigtableWrite.Bulk.DefaultFlushInterval
  ): ClosedTap[Nothing] =
    self.write(BigtableWrite[T](bigtableOptions, tableId))(
      BigtableWrite.Bulk(numOfShards, flushInterval)
    )
}

final class BigtableTypedOps[K: Coder, T: BigtableType: Coder](
  private val self: SCollection[(K, T)]
) {

  def saveAsBigtable(
    projectId: String,
    instanceId: String,
    tableId: String,
    columnFamily: String,
    keyFn: K => ByteString
  ): ClosedTap[Nothing] = {
    val params = BigtableTypedIO.WriteParam[K](columnFamily, keyFn)
    self.write(BigtableTypedIO[K, T](BTOptions(projectId, instanceId), tableId))(params)
  }

  def saveAsBigtable(
    projectId: String,
    instanceId: String,
    tableId: String,
    columnFamily: String,
    keyFn: K => ByteString,
    timestamp: Long
  ): ClosedTap[Nothing] = {
    val params = BigtableTypedIO.WriteParam[K](columnFamily, keyFn, timestamp)
    self.write(BigtableTypedIO[K, T](BTOptions(projectId, instanceId), tableId))(params)
  }

  def saveAsBigtable(
    bigtableOptions: BigtableOptions,
    tableId: String,
    columnFamily: String,
    keyFn: K => ByteString,
    timestamp: Long = BigtableTypedIO.WriteParam.DefaultTimestamp,
    numOfShards: Int,
    flushInterval: Duration = BigtableTypedIO.WriteParam.DefaultFlushInterval
  ): ClosedTap[Nothing] = {
    val params =
      BigtableTypedIO
        .WriteParam[K](columnFamily, keyFn, timestamp, Some(numOfShards), flushInterval)
    self.write(BigtableTypedIO[K, T](bigtableOptions, tableId))(params)
  }
}

trait SCollectionSyntax {
  implicit def bigtableMutationOps[T <: Mutation](
    sc: SCollection[(ByteString, Iterable[T])]
  ): SCollectionMutationOps[T] = new SCollectionMutationOps[T](sc)

  implicit def bigtableTypedOps[K: Coder, T: BigtableType: Coder](
    sc: SCollection[(K, T)]
  ): BigtableTypedOps[K, T] = new BigtableTypedOps[K, T](sc)
}
