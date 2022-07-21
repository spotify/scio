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

import com.google.protobuf.Message
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.util.{ProtobufUtil, ScioUtil}
import com.spotify.scio.values.SCollection

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

final case class ProtobufIO[T <: Message: ClassTag](path: String) extends ScioIO[T] {
  override type ReadP = Unit
  override type WriteP = ProtobufIO.WriteParam
  final override val tapT: TapT.Aux[T, T] = TapOf[T]
  private val protoCoder = Coder.protoMessageCoder[T]

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(ObjectFileIO[T](path)(protoCoder))

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val metadata = params.metadata ++ ProtobufUtil.schemaMetadataOf[T]
    data.write(ObjectFileIO[T](path)(protoCoder))(params.copy(metadata = metadata)).underlying
  }

  override def tap(read: ReadP): Tap[T] =
    ObjectFileTap[T](ScioUtil.addPartSuffix(path))(protoCoder)
}

object ProtobufIO {
  type WriteParam = AvroIO.WriteParam
  val WriteParam = AvroIO.WriteParam
}