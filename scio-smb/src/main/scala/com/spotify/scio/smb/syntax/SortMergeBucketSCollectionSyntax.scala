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

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.values._
import org.apache.beam.sdk.extensions.smb.SortedBucketSink
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection

trait SortMergeBucketSCollectionSyntax {
  implicit def toSortMergeBucketSCollection[T: Coder](
    data: SCollection[T]
  ): SortedBucketSCollection[T] =
    new SortedBucketSCollection(data)
}

private[smb] object SortMergeBucketSCollectionSyntax {
  // Adapted from ScioUtil
  def toPathFragment(path: String, suffix: String): String =
    if (path.endsWith("/")) s"${path}bucket-*-shard-*${suffix}"
    else s"$path/bucket-*-shard-${suffix}"
}

final class SortedBucketSCollection[T: Coder](private val self: SCollection[T]) {
  type Write = PTransform[PCollection[T], SortedBucketSink.WriteResult]

  // @Todo: Implement taps for metadata/bucket elements
  def saveAsSortedBucket(write: Write): ClosedTap[Nothing] = {
    self.applyInternal(write)
    ClosedTap[Nothing](EmptyTap)
  }
}
