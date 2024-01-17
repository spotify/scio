/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.avro

import com.google.protobuf.Message
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.protobuf.util.ProtobufUtil
import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag

final case class ProtobufIO[T <: Message: ClassTag](path: String) extends ScioIO[T] {
  override type ReadP = ProtobufIO.ReadParam
  override type WriteP = ProtobufIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  override def testId: String = s"ProtobufIO($path)"

  private lazy val underlying: ObjectFileIO[T] = ObjectFileIO(path)

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(underlying)(params)

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val metadata = params.metadata ++ ProtobufUtil.schemaMetadataOf[T]
    data.write(underlying)(params.copy(metadata = metadata)).underlying
  }

  override def tap(read: ReadP): Tap[T] =
    ProtobufFileTap(path, read)
}

object ProtobufIO {
  type ReadParam = GenericRecordIO.ReadParam
  val ReadParam = GenericRecordIO.ReadParam
  type WriteParam = GenericRecordIO.WriteParam
  val WriteParam = GenericRecordIO.WriteParam
}
