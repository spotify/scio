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

import com.spotify.scio.ScioContext
import com.spotify.scio.managed.ManagedIO
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row

class ManagedScioContextSyntax(self: ScioContext) {
  def managed(source: String, schema: Schema, config: Map[String, Object] = Map.empty): SCollection[Row] =
    self.read[Row](ManagedIO(source, config))(ManagedIO.ReadParam(schema))
}

trait ScioContextSyntax {
  implicit def managedScioContextSyntax(self: ScioContext): ManagedScioContextSyntax =
    new ManagedScioContextSyntax(self)
}
