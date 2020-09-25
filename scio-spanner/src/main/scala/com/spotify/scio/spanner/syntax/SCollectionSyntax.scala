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
package com.spotify.scio.spanner.syntax

import com.google.cloud.spanner.Mutation
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.spanner.SpannerWrite
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode

final class SpannerSCollectionOps(private val self: SCollection[Mutation]) extends AnyVal {
  import SpannerWrite._

  def saveAsSpanner(
    spannerConfig: SpannerConfig,
    failureMode: FailureMode = WriteParam.DefaultFailureMode,
    batchSizeBytes: Long = WriteParam.DefaultBatchSizeBytes
  ): ClosedTap[Nothing] =
    self.write(SpannerWrite(spannerConfig))(WriteParam(failureMode, batchSizeBytes))
}

trait SCollectionSyntax {
  implicit def spannerSCollectionOps(s: SCollection[Mutation]): SpannerSCollectionOps =
    new SpannerSCollectionOps(s)
}
