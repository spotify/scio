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
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.values._
import org.apache.beam.sdk.extensions.smb.SortedBucketSink
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection

trait SortMergeBucketSCollectionSyntax {
  implicit def toSortMergeBucketSCollection[T](
    data: SCollection[T]
  ): SortedBucketSCollection[T] = new SortedBucketSCollection(data)
}

final class SortedBucketSCollection[T](private val self: SCollection[T]) {
  type Write = PTransform[PCollection[T], SortedBucketSink.WriteResult]

  /**
   * Save an `SCollection[T]` to a filesystem, where each file represents a bucket
   * whose records are lexicographically sorted by some key specified in the
   * [[org.apache.beam.sdk.extensions.smb.BucketMetadata]] corresponding to the provided
   * [[SortedBucketSink]] transform.
   *
   * @param write the [[PTransform]] that applies a [[SortedBucketSink]] transform to the input
   *              data. It contains information about key function, bucket and shard size, etc.
   */
  @experimental
  def saveAsSortedBucket(write: Write): ClosedTap[Nothing] = {
    self.applyInternal(write)

    // @Todo: Implement taps for metadata/bucket elements
    ClosedTap[Nothing](EmptyTap)
  }
}
