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
import com.google.protobuf.ByteString
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import org.joda.time.Duration
import com.spotify.scio.bigtable.BigtableWrite
import org.apache.beam.sdk.transforms.errorhandling.{BadRecord, ErrorHandler}

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Bigtable methods.
 */
final class SCollectionMutationOps[T <: Mutation](
  private val self: SCollection[(ByteString, Iterable[T])]
) {

  /** Save this SCollection as a Bigtable table. Note that elements must be of type `Mutation`. */
  def saveAsBigtable(
    projectId: String,
    instanceId: String,
    tableId: String,
    flowControl: Boolean = BigtableWrite.WriteParam.DefaultFlowControl,
    errorHandler: ErrorHandler[BadRecord, _] = BigtableWrite.WriteParam.DefaultErrorHandler,
    appProfileId: String = BigtableWrite.WriteParam.DefaultAppProfileId,
    attemptTimeout: Duration = BigtableWrite.WriteParam.DefaultAttemptTimeout,
    operationTimeout: Duration = BigtableWrite.WriteParam.DefaultOperationTimeout,
    maxBytesPerBatch: Option[Long] = BigtableWrite.WriteParam.DefaultMaxBytesPerBatch,
    maxElementsPerBatch: Option[Long] = BigtableWrite.WriteParam.DefaultMaxElementsPerBatch,
    maxOutstandingBytes: Option[Long] = BigtableWrite.WriteParam.DefaultMaxOutstandingBytes,
    maxOutstandingElements: Option[Long] = BigtableWrite.WriteParam.DefaultMaxOutstandingElements
  ): ClosedTap[Nothing] = {
    val param = BigtableWrite.WriteParam(
      flowControl,
      errorHandler,
      appProfileId,
      attemptTimeout,
      operationTimeout,
      maxBytesPerBatch,
      maxElementsPerBatch,
      maxOutstandingBytes,
      maxOutstandingElements
    )

    self.write(BigtableWrite[T](projectId, instanceId, tableId))(param)
  }
}

trait SCollectionSyntax {
  implicit def bigtableMutationOps[T <: Mutation](
    sc: SCollection[(ByteString, Iterable[T])]
  ): SCollectionMutationOps[T] = new SCollectionMutationOps[T](sc)
}
