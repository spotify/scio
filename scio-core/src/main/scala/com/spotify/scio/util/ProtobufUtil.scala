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
  // This must be in implicit scope when using .toAvro
  lazy val genericRecordMessageCoder: Coder[GenericRecord] =
    Coder.avroGenericRecordCoder(AvroBytesUtil.schema)

  lazy val genericRecordMessageSchema: Schema = AvroBytesUtil.schema

  def schemaMetadataOf[T <: Message: ClassTag]: Map[String, AnyRef] = {
    import me.lyh.protobuf.generic
    val schema = generic.Schema
      .of[Message](classTag[T].asInstanceOf[ClassTag[Message]])
      .toJson

    Map("protobuf.generic.schema" -> schema)
  }

  /**
   * A function that converts a Protobuf Message of type `T` into a GenericRecord
   * with a single schema field containing the Message's serialized bytes.
   * @return A function mapping a Protobuf message to an Avro record
   */
  def toAvro[T <: Message: ClassTag]: T => GenericRecord = {
    val protoCoder = CoderMaterializer.beamWithDefault(protoCoderOf[T])

    (t: T) => AvroBytesUtil.encode(protoCoder, t)
  }

  private[scio] def protoCoderOf[T <: Message: ClassTag]: Coder[T] =
    Coder
      .protoMessageCoder[Message](classTag[T].asInstanceOf[ClassTag[Message]])
      .asInstanceOf[Coder[T]]
}
