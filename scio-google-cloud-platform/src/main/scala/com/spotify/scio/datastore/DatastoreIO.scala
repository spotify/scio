/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.datastore

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT}
import com.google.datastore.v1.{Entity, Query}
import org.apache.beam.sdk.io.gcp.{datastore => beam}

final case class DatastoreIO(projectId: String) extends ScioIO[Entity] {
  override type ReadP = DatastoreIO.ReadParam
  override type WriteP = Unit

  override val tapT: TapT.Aux[Entity, Nothing] = EmptyTapOf[Entity]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Entity] =
    sc.applyTransform(
      beam.DatastoreIO
        .v1()
        .read()
        .withProjectId(projectId)
        .withNamespace(params.namespace)
        .withQuery(params.query)
    )

  override protected def write(data: SCollection[Entity], params: WriteP): Tap[Nothing] = {
    data.applyInternal(beam.DatastoreIO.v1.write.withProjectId(projectId))
    EmptyTap
  }

  override def tap(read: DatastoreIO.ReadParam): Tap[Nothing] = EmptyTap
}

object DatastoreIO {
  final case class ReadParam(query: Query, namespace: String = null)
}
