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
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.io.Tap
import com.spotify.scio.values._
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Enhanced version of [[SCollection]] with Avro methods. */
final class AvroSCollection[T](@transient val self: SCollection[T]) extends Serializable {

  import self.ct

  /**
   * Save this SCollection as an Avro file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   */
  def saveAsAvroFile(path: String,
                     numShards: Int = 0,
                     schema: Schema = null,
                     suffix: String = "",
                     codec: CodecFactory = CodecFactory.deflateCodec(6),
                     metadata: Map[String, AnyRef] = Map.empty)
  : Future[Tap[T]] = {
    val param = nio.AvroIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(nio.AvroIO[T](path, schema))(param)
  }

  /**
   * Save this SCollection as an Avro file. Note that element type `T` must be a case class
   * annotated with [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   */
  def saveAsTypedAvroFile(path: String,
                          numShards: Int = 0,
                          suffix: String = "",
                          codec: CodecFactory = CodecFactory.deflateCodec(6),
                          metadata: Map[String, AnyRef] = Map.empty)
                         (implicit ct: ClassTag[T], tt: TypeTag[T], ev: T <:< HasAvroAnnotation)
  : Future[Tap[T]] = {
    val param = nio.AvroIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(nio.Typed.AvroIO[T](path))(param)
  }

  /**
   * Save this SCollection as an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  def saveAsObjectFile(path: String, numShards: Int = 0, suffix: String = ".obj",
                       codec: CodecFactory = CodecFactory.deflateCodec(6),
                       metadata: Map[String, AnyRef] = Map.empty): Future[Tap[T]] = {
    val param = nio.ObjectFileIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(nio.ObjectFileIO[T](path))(param)
  }

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   */
  def saveAsProtobufFile(path: String, numShards: Int = 0, suffix: String = ".protobuf",
                         codec: CodecFactory = CodecFactory.deflateCodec(6),
                         metadata: Map[String, AnyRef] = Map.empty)
                        (implicit ev: T <:< Message): Future[Tap[T]] = {
    val param = nio.ProtobufIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(nio.ProtobufIO[T](path))(param)
  }
}
