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

package com.spotify.scio.coders.instances

import com.google.api.services.bigquery.model.TableRow
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.RowCoder
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage, PubsubMessageWithAttributesCoder}
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow, PaneInfo}
import org.apache.beam.sdk.values.{KV, Row}

trait BeamTypeCoders {
  implicit def intervalWindowCoder: Coder[IntervalWindow] = Coder.beam(IntervalWindow.getCoder)

  implicit def boundedWindowCoder: Coder[BoundedWindow] = Coder.kryo[BoundedWindow]

  implicit def paneInfoCoder: Coder[PaneInfo] = Coder.beam(PaneInfo.PaneInfoCoder.of())

  implicit def tableRowCoder: Coder[TableRow] = Coder.beam(TableRowJsonCoder.of())

  def row(schema: BSchema): Coder[Row] = Coder.beam(RowCoder.of(schema))

  implicit def messageCoder: Coder[PubsubMessage] =
    Coder.beam(PubsubMessageWithAttributesCoder.of())

  implicit def beamKVCoder[K: Coder, V: Coder]: Coder[KV[K, V]] = Coder.kv(Coder[K], Coder[V])
}

private[coders] object BeamTypeCoders extends BeamTypeCoders
