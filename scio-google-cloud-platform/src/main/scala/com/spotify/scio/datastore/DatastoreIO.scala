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
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.io.gcp.datastore.{DatastoreIO => BDatastoreIO, DatastoreV1 => BDatastore}

final case class DatastoreIO(projectId: String) extends ScioIO[Entity] {
  override type ReadP = DatastoreIO.ReadParam
  override type WriteP = DatastoreIO.WriteParam

  override val tapT: TapT.Aux[Entity, Nothing] = EmptyTapOf[Entity]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Entity] = {
    val coder = CoderMaterializer.beam(sc, Coder.protoMessageCoder[Entity])
    val read = BDatastoreIO
      .v1()
      .read()
      .withProjectId(projectId)
      .withNamespace(params.namespace)
      .withQuery(params.query)
    sc.applyTransform(
      Option(params.configOverride).map(_(read)).getOrElse(read)
    ).setCoder(coder)
  }

  override protected def write(data: SCollection[Entity], params: WriteP): Tap[Nothing] = {
    val write = BDatastoreIO.v1.write.withProjectId(projectId)
    data.applyInternal(
      Option(params.configOverride).map(_(write)).getOrElse(write)
    )
    EmptyTap
  }

  override def tap(read: DatastoreIO.ReadParam): Tap[Nothing] = EmptyTap
}

object DatastoreIO {

  object ReadParam {
    val DefaultNamespace: String = null
    val DefaultConfigOverride: BDatastore.Read => BDatastore.Read = identity
  }

  final case class ReadParam private (
    query: Query,
    namespace: String = ReadParam.DefaultNamespace,
    configOverride: BDatastore.Read => BDatastore.Read = ReadParam.DefaultConfigOverride
  )

  object WriteParam {
    val DefaultConfigOverride: BDatastore.Write => BDatastore.Write = identity
  }

  final case class WriteParam private (
    configOverride: BDatastore.Write => BDatastore.Write = WriteParam.DefaultConfigOverride
  )
}
