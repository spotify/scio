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

package com.spotify.scio.avro.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import com.google.protobuf.Message
import com.twitter.chill.Externalizer
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.io._
import com.spotify.scio.util._
import com.spotify.scio.values._
import com.spotify.scio.avro.types.AvroType
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.AvroBytesUtil

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Tap for Avro files.
 * @param schema must be not null if `T` is of type
 *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
 */
case class AvroTap[T: ClassTag](path: String,
                                @transient private val schema: Schema = null) extends Tap[T] {
  private lazy val s = Externalizer(schema)
  override def value: Iterator[T] = FileStorage(path).avroFile[T](s.get)
  override def open(sc: ScioContext): SCollection[T] = sc.avroFile[T](path, s.get)
}

/**
 * Tap for object files. Note that serialization is not guaranteed to be compatible across Scio
 * releases.
 */
case class ObjectFileTap[T: ClassTag](path: String) extends Tap[T] {
  override def value: Iterator[T] = {
    val elemCoder = ScioUtil.getScalaCoder[T]
    FileStorage(path).avroFile[GenericRecord](AvroBytesUtil.schema).map { r =>
      AvroBytesUtil.decode(elemCoder, r)
    }
  }
  override def open(sc: ScioContext): SCollection[T] = sc.objectFile(path)
}

final case class AvroTaps(self: Taps) {

  import self.{mkTap, isPathDone}

  /** Get a `Future[Tap[T]]` of a Protobuf file. */
  def protobufFile[T: ClassTag](path: String)(implicit ev: T <:< Message): Future[Tap[T]] =
    mkTap(s"Protobuf: $path", () => isPathDone(path), () => ObjectFileTap[T](path))

  /** Get a `Future[Tap[T]]` of an object file. */
  def objectFile[T: ClassTag](path: String): Future[Tap[T]] =
    mkTap(s"Object file: $path", () => isPathDone(path), () => ObjectFileTap[T](path))

  /** Get a `Future[Tap[T]]` for an Avro file. */
  def avroFile[T: ClassTag](path: String, schema: Schema = null): Future[Tap[T]] =
    mkTap(s"Avro: $path", () => isPathDone(path), () => AvroTap[T](path, schema))

  /** Get a `Future[Tap[T]]` for typed Avro source. */
  def typedAvroFile[T <: HasAvroAnnotation : TypeTag: ClassTag](path: String): Future[Tap[T]] = {
    val avroT = AvroType[T]

    import scala.concurrent.ExecutionContext.Implicits.global
    avroFile[GenericRecord](path, avroT.schema)
      .map(_.map(avroT.fromGenericRecord))
  }
}
