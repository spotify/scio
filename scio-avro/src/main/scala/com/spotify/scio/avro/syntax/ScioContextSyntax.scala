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

package com.spotify.scio.avro.syntax

import com.google.protobuf.Message
import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values._
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Enhanced version of [[ScioContext]] with Avro methods. */
final class ScioContextOps(private val self: ScioContext) extends AnyVal {

  /**
   * Get an SCollection for an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  def objectFile[T: Coder](path: String): SCollection[T] =
    self.read(ObjectFileIO[T](path))

  /**
   * Get an SCollection of type [[org.apache.avro.generic.GenericRecord GenericRecord]] for an Avro
   * file.
   */
  def avroFile[T: ClassTag: Coder](path: String, schema: Schema): SCollection[T] =
    self.read(GenericRecordIO[T](path, schema))

  /**
   * Get an SCollection of type [[org.apache.avro.specific.SpecificRecordBase SpecificRecordBase]]
   * for an Avro file.
   */
  def avroFile[T <: SpecificRecordBase: ClassTag: Coder](path: String): SCollection[T] =
    self.read(SpecificRecordIO[T](path))

  /**
   * Get a typed SCollection from an Avro schema.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.avro.types.AvroType AvroType.fromSchema]],
   * [[com.spotify.scio.avro.types.AvroType AvroType.fromPath]], or
   * [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   */
  def typedAvroFile[T <: HasAvroAnnotation: ClassTag: TypeTag: Coder](
    path: String): SCollection[T] =
    self.read(AvroTyped.AvroIO[T](path))

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   */
  def protobufFile[T <: Message: ClassTag: Coder](path: String): SCollection[T] =
    self.read(ProtobufIO[T](path))
}

/** Enhanced with Avro methods. */
trait ScioContextSyntax {
  implicit def avroScioContextOps(c: ScioContext): ScioContextOps = new ScioContextOps(c)
}
