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

package com.spotify.scio.datastore.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.datastore.DatastoreIO
import com.google.datastore.v1.{Entity, Query}
import com.spotify.scio.datastore.DatastoreIO.ReadParam
import org.apache.beam.sdk.io.gcp.datastore.{DatastoreV1 => BDatastore}

final class ScioContextOps(private val sc: ScioContext) extends AnyVal {

  /**
   * Get an SCollection for a Datastore query.
   * @group input
   */
  def datastore(
    projectId: String,
    query: Query,
    namespace: String = ReadParam.DefaultNamespace,
    configOverride: BDatastore.Read => BDatastore.Read = ReadParam.DefaultConfigOverride
  ): SCollection[Entity] =
    sc.read(DatastoreIO(projectId))(ReadParam(query, namespace, configOverride))
}

trait ScioContextSyntax {
  implicit def datastoreScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
