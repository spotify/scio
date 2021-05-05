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

package com.spotify.scio.util

import com.google.protobuf.Message
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.reflect.{classTag, ClassTag}

object ProtobufUtil {

  /**
   * A Coder for Protobuf [[Message]]s encoded as Avro [[GenericRecord]]s.
   * This must be in implicit scope when using [[ProtobufUtil.toAvro]], for example:
   *
   * `implicit val avroMessageCoder: Coder[GenericRecord] = ProtobufUtil.AvroMessageCoder`
   */
  lazy val AvroMessageCoder: Coder[GenericRecord] =
    Coder.avroGenericRecordCoder(AvroBytesUtil.schema)

  /** The Avro [[Schema]] corresponding to an Avro-encoded Protobuf [[Message]]. */
  lazy val AvroMessageSchema: Schema = AvroBytesUtil.schema

  /**
   * A metadata map containing information about the underlying Protobuf schema of the
   * [[Message]] bytes encoded inside [[AvroMessageSchema]]'s `bytes` field.
   *
   * @tparam T subclass of [[Message]]
   */
  def schemaMetadataOf[T <: Message: ClassTag]: Map[String, AnyRef] = {
    import me.lyh.protobuf.generic
    import me.lyh.protobuf.generic.JsonSchema
    val schema = generic.Schema
      .of[Message](classTag[T].asInstanceOf[ClassTag[Message]])
      .toJson

    Map("protobuf.generic.schema" -> schema)
  }

  /**
   * A function that converts a Protobuf [[Message]] of type `T` into a [[GenericRecord]]
   * whose [[Schema]] is a single byte array field, corresponding to the serialized bytes in `T`.
   */
  def toAvro[T <: Message: ClassTag]: T => GenericRecord = {
    val protoCoder = CoderMaterializer.beamWithDefault(Coder.protoMessageCoder[T])

    (t: T) => AvroBytesUtil.encode(protoCoder, t)
  }
}
