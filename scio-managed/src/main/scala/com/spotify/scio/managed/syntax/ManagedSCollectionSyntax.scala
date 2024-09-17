/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.managed.syntax

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.managed.ManagedIO
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.values.Row

class ManagedSCollectionSyntax(self: SCollection[Row]) {
  def saveAsManaged(sink: String, config: Map[String, AnyRef] = Map.empty): ClosedTap[Nothing] =
    self.write(ManagedIO(sink, config))(ManagedIO.WriteParam())
}

trait SCollectionSyntax {
  implicit def managedSCollectionSyntax(self: SCollection[Row]): ManagedSCollectionSyntax =
    new ManagedSCollectionSyntax(self)
}
