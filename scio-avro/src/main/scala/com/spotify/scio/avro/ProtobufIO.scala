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
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT, TestIO}
import com.spotify.scio.protobuf.util.ProtobufUtil
import com.spotify.scio.values.SCollection
import magnolify.protobuf.ProtobufType

import scala.reflect.ClassTag

sealed trait ProtobufIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, T] = TapOf[T]
}

object ProtobufIO {
  final def apply[T](path: String): ProtobufIO[T] =
    new ProtobufIO[T] with TestIO[T] {
      override def testId: String = s"ProtobufIO($path)"
    }
}

object ProtobufObjectFileIO {
  type ReadParam = GenericRecordIO.ReadParam
  val ReadParam = GenericRecordIO.ReadParam
  type WriteParam = GenericRecordIO.WriteParam
  val WriteParam = GenericRecordIO.WriteParam
}

final case class ProtobufObjectFileIO[T <: Message: ClassTag](path: String) extends ProtobufIO[T] {
  override type ReadP = ProtobufObjectFileIO.ReadParam
  override type WriteP = ProtobufObjectFileIO.WriteParam
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

  override def tap(read: ReadP): Tap[T] = ProtobufFileTap(path, read)
}

final case class TypedProtobufObjectFileIO[T: Coder, U <: Message: ClassTag](
  path: String,
  pt: ProtobufType[T, U]
) extends ProtobufIO[T] {
  override type ReadP = TypedProtobufObjectFileIO.ReadParam
  override type WriteP = TypedProtobufObjectFileIO.WriteParam
  override def testId: String = s"ProtobufIO($path)"

  private lazy val underlying: ObjectFileIO[U] = ObjectFileIO(path)

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.read(underlying)(params).map(u => pt.from(u))

  /**
   * Save this SCollection as a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val metadata = params.metadata ++ ProtobufUtil.schemaMetadataOf[U]
    data
      .map(t => pt.to(t))
      .write(underlying)(params.copy(metadata = metadata))
      .underlying
      .map(u => pt.from(u))
  }

  override def tap(read: ReadP): Tap[T] = ProtobufFileTap[U](path, read).map(u => pt.from(u))
}

object TypedProtobufObjectFileIO {
  type ReadParam = ProtobufObjectFileIO.ReadParam
  val ReadParam = ProtobufObjectFileIO.ReadParam
  type WriteParam = ProtobufObjectFileIO.WriteParam
  val WriteParam = ProtobufObjectFileIO.WriteParam
}
