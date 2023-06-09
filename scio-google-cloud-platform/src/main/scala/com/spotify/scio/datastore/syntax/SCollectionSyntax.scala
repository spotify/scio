/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
import com.google.datastore.v1.Entity
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

package com.spotify.scio.datastore.syntax

import com.spotify.scio.values.SCollection
import com.spotify.scio.datastore.DatastoreIO
import com.spotify.scio.io.ClosedTap
import com.google.datastore.v1.Entity
import com.spotify.scio.datastore.DatastoreIO.WriteParam
import org.apache.beam.sdk.io.gcp.datastore.{DatastoreV1 => BDatastore}

final class SCollectionEntityOps[T <: Entity](private val coll: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection as a Datastore dataset. Note that elements must be of type `Entity`.
   * @group output
   */
  def saveAsDatastore(
    projectId: String,
    configOverride: BDatastore.Write => BDatastore.Write = WriteParam.DefaultConfigOverride
  ): ClosedTap[Nothing] =
    coll.covary_[Entity].write(DatastoreIO(projectId))(WriteParam(configOverride))
}

trait SCollectionSyntax {
  implicit def datastoreEntitySCollectionOps[T <: Entity](
    coll: SCollection[T]
  ): SCollectionEntityOps[T] = new SCollectionEntityOps(coll)
}
