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

import com.spotify.scio.smb.io.SortMergeBucketRead
import com.spotify.scio.testing.JobTest
import com.spotify.scio.testing.JobTest.Builder
import org.apache.beam.sdk.extensions.smb.{BucketMetadata, SortedBucketIO}

trait SortedBucketJobTestSyntax {
  implicit def toSMBJobTestBuilder(jobTest: JobTest.Builder): SortedBucketJobTestBuilder =
    new SortedBucketJobTestBuilder(jobTest)
}

final class SortedBucketJobTestBuilder(private val self: JobTest.Builder) extends AnyVal {
  def inputSmb[K, T](
    io: SortMergeBucketRead[K, T],
    metadata: BucketMetadata[K, T],
    value: Iterable[T]
  ): Builder = self.input(io, value.map(metadata.extractKey).zip(value))

  def inputSmb[K, T](
    read: SortedBucketIO.Read[T],
    metadata: BucketMetadata[K, T],
    value: Iterable[T]
  ): Builder = {
    val io = SortMergeBucketRead[K, T](read.toBucketedInput.getTupleTag.getId)

    self.input(io, value.map(metadata.extractKey).zip(value))
  }
}
