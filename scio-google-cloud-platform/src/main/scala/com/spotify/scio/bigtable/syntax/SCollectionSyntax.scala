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

import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration
import com.spotify.scio.bigtable.BigtableWrite
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import org.apache.hadoop.hbase.client.Mutation

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Bigtable methods.
 */
final class SCollectionMutationOps[T <: Mutation](private val self: SCollection[T]) {

  /** Save this SCollection as a Bigtable table. Note that elements must be of type `Mutation`. */
  def saveAsBigtable(projectId: String, instanceId: String, tableId: String): ClosedTap[Nothing] =
    self.write(BigtableWrite[T](projectId, instanceId, tableId))

  /** Save this SCollection as a Bigtable table. Note that elements must be of type `Mutation`. */
  def saveAsBigtable(config: CloudBigtableTableConfiguration): ClosedTap[Nothing] =
    self.write(BigtableWrite[T](config))
}

trait SCollectionSyntax {
  implicit def bigtableMutationOps[T <: Mutation](sc: SCollection[T]): SCollectionMutationOps[T] =
    new SCollectionMutationOps[T](sc)
}
