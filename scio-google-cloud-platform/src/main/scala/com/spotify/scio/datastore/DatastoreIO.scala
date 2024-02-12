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
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT, TestIO}
import com.google.datastore.v1.{Entity, Query}
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.datastore.TypedDatastoreIO.{ReadParam, WriteParam}
import magnolify.datastore.EntityType
import org.apache.beam.sdk.io.gcp.datastore.{DatastoreIO => BDatastoreIO, DatastoreV1 => BDatastore}

sealed trait DatastoreIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]
}

object DatastoreIO {
  final def apply[T](projectId: String): DatastoreIO[T] =
    new DatastoreIO[T] with TestIO[T] {
      override def testId: String = s"DatastoreIO($projectId)"
    }
}

final case class TypedDatastoreIO[T: EntityType: Coder](projectId: String) extends DatastoreIO[T] {
  override type ReadP = TypedDatastoreIO.ReadParam
  override type WriteP = TypedDatastoreIO.WriteParam
  override def testId: String = s"DatastoreIO($projectId)"

  override protected def read(sc: ScioContext, params: ReadParam): SCollection[T] = {
    val entityType: EntityType[T] = implicitly
    sc.transform { ctx =>
      EntityDatastoreIO
        .read(ctx, projectId, params.namespace, params.query, params.configOverride)
        .map(e => entityType(e))
    }
  }

  override protected def write(data: SCollection[T], params: WriteParam): Tap[Nothing] = {
    val entityType: EntityType[T] = implicitly
    val write = BDatastoreIO.v1.write.withProjectId(projectId)
    data.transform_ { scoll =>
      scoll
        .map(t => entityType(t))
        .applyInternal(
          Option(params.configOverride).map(_(write)).getOrElse(write)
        )
    }
    EmptyTap
  }

  override def tap(read: ReadParam): Tap[Nothing] = EmptyTap
}

object TypedDatastoreIO {
  type ReadParam = EntityDatastoreIO.ReadParam
  val ReadParam = EntityDatastoreIO.ReadParam
  type WriteParam = EntityDatastoreIO.WriteParam
  val WriteParam = EntityDatastoreIO.WriteParam
}

final case class EntityDatastoreIO(projectId: String) extends DatastoreIO[Entity] {
  override type ReadP = EntityDatastoreIO.ReadParam
  override type WriteP = EntityDatastoreIO.WriteParam
  override def testId: String = s"DatastoreIO($projectId)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Entity] =
    EntityDatastoreIO.read(sc, projectId, params.namespace, params.query, params.configOverride)

  override protected def write(data: SCollection[Entity], params: WriteP): Tap[Nothing] = {
    val write = BDatastoreIO.v1.write.withProjectId(projectId)
    data.applyInternal(
      Option(params.configOverride).map(_(write)).getOrElse(write)
    )
    EmptyTap
  }

  override def tap(read: EntityDatastoreIO.ReadParam): Tap[Nothing] = EmptyTap
}

object EntityDatastoreIO {

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

  private[scio] def read(
    sc: ScioContext,
    projectId: String,
    namespace: String,
    query: Query,
    configOverride: BDatastore.Read => BDatastore.Read
  ): SCollection[Entity] = {
    val coder = CoderMaterializer.beam(sc, Coder.protoMessageCoder[Entity])
    val read = BDatastoreIO
      .v1()
      .read()
      .withProjectId(projectId)
      .withNamespace(namespace)
      .withQuery(query)
    sc.applyTransform(
      Option(configOverride).map(_(read)).getOrElse(read)
    ).setCoder(coder)
  }
}
