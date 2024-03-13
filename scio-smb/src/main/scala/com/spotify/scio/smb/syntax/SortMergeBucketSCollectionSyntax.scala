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

package com.spotify.scio.smb.syntax

import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ClosedTap, TapOf}
import com.spotify.scio.smb.SmbIO
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.values._
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, SortedBucketIOUtil}
import org.apache.beam.sdk.values.KV

trait SortMergeBucketSCollectionSyntax {
  implicit def toSortMergeBucketKeyedSCollection[K, V](
    data: SCollection[KV[K, V]]
  ): SortedBucketPairSCollection[K, V] = new SortedBucketPairSCollection(data)

  implicit def toSortMergeBucketSCollection[T](
    data: SCollection[T]
  ): SortedBucketSCollection[T] = new SortedBucketSCollection(data)
}

final class SortedBucketSCollection[T](private val self: SCollection[T]) {

  /**
   * Save an `SCollection[T]` to a filesystem, where each file represents a bucket whose records are
   * lexicographically sorted by some key specified in the
   * [[org.apache.beam.sdk.extensions.smb.BucketMetadata]] corresponding to the provided
   * [[SortedBucketSink]] transform.
   *
   * @param write
   *   the [[PTransform]] that applies a [[SortedBucketSink]] transform to the input data. It
   *   contains information about key function, bucket and shard size, etc.
   */
  @experimental
  def saveAsSortedBucket(
    write: SortedBucketIO.Write[_, _, T]
  ): ClosedTap[T] = {
    import self.coder

    if (self.context.isTest) {
      TestDataManager.getOutput(self.context.testId.get)(SortedBucketIOUtil.testId(write))(self)
      ClosedTap(TapOf[T].saveForTest(self))
    } else {
      val writeResult = self.applyInternal(write)

      ClosedTap(SmbIO.tap(write.getFileOperations, writeResult).apply(self.context))
    }
  }
}

final class SortedBucketPairSCollection[K, V](private val self: SCollection[KV[K, V]]) {

  /**
   * Save an `SCollection[(K, V)]` to a filesystem, where each file represents a bucket whose
   * records are lexicographically sorted by some key specified in the
   * [[org.apache.beam.sdk.extensions.smb.BucketMetadata]] corresponding to the provided
   * [[SortedBucketSink]] transform and to the key K of each KV pair in this `SCollection`.
   *
   * @param write
   *   the [[PTransform]] that applies a [[SortedBucketSink]] transform to the input data. It
   *   contains information about key function, bucket and shard size, etc.
   * @param verifyKeyExtraction
   *   if set, the SMB Sink will add two additional nodes to the job graph to sample this
   *   SCollection and verify that each key K in the collection matches the result of the given
   *   [[org.apache.beam.sdk.extensions.smb.BucketMetadata]] 's `extractKey` function.
   */
  @experimental
  def saveAsPreKeyedSortedBucket(
    write: SortedBucketIO.Write[K, Void, V],
    verifyKeyExtraction: Boolean = true
  ): ClosedTap[V] = {
    val beamValueCoder = self.internal.getCoder.asInstanceOf[KvCoder[K, V]].getValueCoder
    implicit val valueCoder: Coder[V] = Coder.beam(beamValueCoder)

    if (self.context.isTest) {
      val data = self.map(_.getValue)
      TestDataManager.getOutput(self.context.testId.get)(SortedBucketIOUtil.testId(write))(data)
      ClosedTap(TapOf[V].saveForTest(data))
    } else {
      val writeResult =
        self.applyInternal(write.onKeyedCollection(beamValueCoder, verifyKeyExtraction))

      ClosedTap(SmbIO.tap(write.getFileOperations, writeResult).apply(self.context))
    }
  }
}
