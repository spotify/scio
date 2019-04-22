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

import com.google.cloud.spanner.Struct
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.spanner.SpannerRead
import com.spotify.scio.spanner.instances.coders._
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig

import scala.language.implicitConversions

final class SpannerScioContextOps(private val self: ScioContext) extends AnyVal {
  import SpannerRead.ReadParam._

  def spannerTable(
    spannerConfig: SpannerConfig,
    table: String,
    columns: Seq[String],
    withBatching: Boolean = DefaultWithBatching,
    withTransaction: Boolean = DefaultWithTransaction
  ): SCollection[Struct] = {
    val params = SpannerRead.ReadParam(
      SpannerRead.FromTable(table, columns),
      withTransaction,
      withBatching
    )

    self.read(SpannerRead(spannerConfig))(params)
  }

  def spannerQuery(
    spannerConfig: SpannerConfig,
    query: String,
    withBatching: Boolean = DefaultWithBatching,
    withTransaction: Boolean = DefaultWithTransaction
  ): SCollection[Struct] = {
    val params = SpannerRead.ReadParam(
      SpannerRead.FromQuery(query),
      withTransaction,
      withBatching
    )

    self.read(SpannerRead(spannerConfig))(params)
  }
}

trait ScioContextSyntax {
  implicit def spannerScioContextOps(sc: ScioContext): SpannerScioContextOps =
    new SpannerScioContextOps(sc)
}
