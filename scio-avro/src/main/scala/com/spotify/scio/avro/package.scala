/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio

import scala.language.implicitConversions
import com.spotify.scio.values._


/**
 * Main package for Avro APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.avro._
 * }}}
 */
package object avro {

  /** Typed Avro annotations and converters. */
  val AvroType = com.spotify.scio.avro.types.AvroType

  /** Annotation for Avro field and record documentation. */
  type doc = com.spotify.scio.avro.types.doc

  type AvroIO[T] = avro.nio.AvroIO[T]
  val AvroIO = avro.nio.AvroIO

  type ObjectFileIO[T] = avro.nio.ObjectFileIO[T]
  val ObjectFileIO = avro.nio.ObjectFileIO

  type ProtobufIO[T] = avro.nio.ProtobufIO[T]
  val ProtobufIO = avro.nio.ProtobufIO

  implicit def toAvroScioContext(c: ScioContext): AvroScioContext =
    new AvroScioContext(c)
  implicit def toAvroSCollection[T](c: SCollection[T]): AvroSCollection[T] =
    new AvroSCollection[T](c)

}
