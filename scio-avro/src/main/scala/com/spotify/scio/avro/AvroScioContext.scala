/*
 * Copyright 2018 Spotify AB.
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

import com.google.protobuf.Message
import org.apache.avro.Schema
import com.spotify.scio.ScioContext
import com.spotify.scio.values._
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

final class AvroScioContext(@transient val self: ScioContext) extends Serializable {

  /**
   * Get an SCollection for an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   * @group input
   */
  def objectFile[T: ClassTag](path: String): SCollection[T] =
    self.read(nio.ObjectFile[T](path))

  /**
   * Get an SCollection for an Avro file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @group input
   */
  def avroFile[T: ClassTag](path: String, schema: Schema = null): SCollection[T] =
    self.read(nio.AvroFile[T](path, schema))

  /**
   * Get a typed SCollection from an Avro schema.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.avro.types.AvroType AvroType.fromSchema]],
   * [[com.spotify.scio.avro.types.AvroType AvroType.fromPath]], or
   * [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   *
   * @group input
   */
  def typedAvroFile[T <: HasAvroAnnotation : ClassTag : TypeTag](path: String)
  : SCollection[T] =
    self.read(nio.Typed.AvroFile[T](path))

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   * @group input
   */
  def protobufFile[T: ClassTag](path: String)(implicit ev: T <:< Message): SCollection[T] =
    self.read(nio.ProtobufFile[T](path))

}
