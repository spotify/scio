/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.avro.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.AvroMagnolifyTyped
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import magnolify.avro.{AvroType => MagnolifyAvroType}

final class MagnolifyAvroScioContextOps(private val self: ScioContext) extends AnyVal {

  def typedAvroFile[T: MagnolifyAvroType: Coder](path: String): SCollection[T] =
    self.read(AvroMagnolifyTyped[T](path))(AvroMagnolifyTyped.ReadParam())

  def typedAvroFile[T: MagnolifyAvroType: Coder](path: String, suffix: String): SCollection[T] =
    self.read(AvroMagnolifyTyped[T](path))(AvroMagnolifyTyped.ReadParam(suffix))
}

trait MagnolifyAvroScioContextSyntax {
  implicit def magnolifyAvroScioContextOps(c: ScioContext): MagnolifyAvroScioContextOps =
    new MagnolifyAvroScioContextOps(c)
}
