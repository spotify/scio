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
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values._
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Enhanced version of [[SCollection]] with Avro methods. */
final class AvroSCollection[T](@transient val self: SCollection[T]) extends Serializable {

  /**
   * Save this SCollection as an Avro file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   */
  // scalastyle:off parameter.number
  def saveAsAvroFile(path: String,
                     numShards: Int = AvroIO.WriteParam.DefaultNumShards,
                     schema: Schema,
                     suffix: String = AvroIO.WriteParam.DefaultSuffix,
                     codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
                     metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata)(
    implicit ev: T <:< GenericRecord,
    coder: Coder[T]): Future[Tap[T]] = {
    val param = AvroIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(AvroIO[T](path, schema))(param)
  }

  def saveAsAvroFile(path: String,
                     numShards: Int,
                     suffix: String,
                     codec: CodecFactory,
                     metadata: Map[String, AnyRef])(implicit ct: ClassTag[T],
                                                    ev: T <:< SpecificRecordBase,
                                                    coder: Coder[T]): Future[Tap[T]] = {
    val cls = ScioUtil.classOf[T]
    val param = AvroIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(AvroIO[T](path))(param)
  }

  /**
   * Save this SCollection as an Avro file. Note that element type `T` must be a case class
   * annotated with [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   */
  // scalastyle:off parameter.number
  def saveAsTypedAvroFile(path: String,
                          numShards: Int = AvroIO.WriteParam.DefaultNumShards,
                          suffix: String = AvroIO.WriteParam.DefaultSuffix,
                          codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
                          metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata)(
    implicit ct: ClassTag[T],
    tt: TypeTag[T],
    ev: T <:< HasAvroAnnotation,
    coder: Coder[T]): Future[Tap[T]] = {
    val param = AvroIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(AvroTyped.AvroIO[T](path))(param)
  }
  // scalastyle:on parameter.number

  /**
   * Save this SCollection as an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  def saveAsObjectFile(path: String,
                       numShards: Int = AvroIO.WriteParam.DefaultNumShards,
                       suffix: String = ".obj",
                       codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
                       metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata)(
    implicit coder: Coder[T]): Future[Tap[T]] = {
    val param = ObjectFileIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(ObjectFileIO[T](path))(param)
  }

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage
   * Avro's block file format.
   */
  def saveAsProtobufFile(path: String,
                         numShards: Int = AvroIO.WriteParam.DefaultNumShards,
                         suffix: String = ".protobuf",
                         codec: CodecFactory = AvroIO.WriteParam.DefaultCodec,
                         metadata: Map[String, AnyRef] = AvroIO.WriteParam.DefaultMetadata)(
    implicit ev: T <:< Message,
    ct: ClassTag[T],
    coder: Coder[T]): Future[Tap[T]] = {
    val param = ProtobufIO.WriteParam(numShards, suffix, codec, metadata)
    self.write(ProtobufIO[T](path))(param)
  }
}
