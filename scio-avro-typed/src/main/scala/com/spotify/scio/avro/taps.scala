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

package com.spotify.scio.avro

import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{Tap, Taps}
import scala.reflect.runtime.universe._
import scala.concurrent.Future

case class TypedAvroTapsOps(self: Taps) {

  /** Get a `Future[Tap[T]]` for typed Avro source. */
  def typedAvroFile[T <: HasAvroAnnotation: TypeTag: Coder](
    path: String
  ): Future[Tap[T]] = {
    val avroT = AvroType[T]

    import scala.concurrent.ExecutionContext.Implicits.global
    AvroTaps(self).avroFile(path, avroT.schema).map(_.map(avroT.fromGenericRecord))
  }
}

trait TypedAvroTapsSyntax {
  implicit def typedAvroTapsOps[T <: HasAvroAnnotation: TypeTag: Coder](
    taps: AvroTaps
  ): TypedAvroTapsOps = TypedAvroTapsOps(taps.self)
}
