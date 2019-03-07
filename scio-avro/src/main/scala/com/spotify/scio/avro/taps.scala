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
import com.spotify.scio._
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.io.{FileStorage, Tap, Taps}
import com.spotify.scio.values._
import com.twitter.chill.Externalizer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

sealed trait AvroTap[T] extends Tap[T]

/**
 * Tap for GenericRecord Avro files.
 *
 */
case class SchemaAvroTap[T: Coder](path: String,
                                   @transient private val
                                   schema: Schema)(implicit ev: T <:< GenericRecord)
    extends AvroTap[T] {
  private lazy val s = Externalizer(schema)

  override def value: Iterator[T] =
    FileStorage(path).avroFile[T](s.get)

  override def open(sc: ScioContext): SCollection[T] = sc.avroFile[T](path, s.get)
}

/**
 * Tap for SpecificRecord Avro files.
 *
 * */
case class SpecificRecordAvroTap[T: ClassTag: Coder](path: String)(
  implicit ev: T <:< SpecificRecordBase)
    extends AvroTap[T] {
  override def value: Iterator[T] = FileStorage(path).avroFile[T]()

  override def open(sc: ScioContext): SCollection[T] = sc.avroFile[T](path)
}

/**
 * Tap for object files. Note that serialization is not guaranteed to be compatible across Scio
 * releases.
 */
case class ObjectFileTap[T: Coder](path: String) extends Tap[T] {
  override def value: Iterator[T] = {
    val elemCoder = CoderMaterializer.beamWithDefault(Coder[T])
    FileStorage(path).avroFile[GenericRecord](AvroBytesUtil.schema).map { r =>
      AvroBytesUtil.decode(elemCoder, r)
    }
  }
  override def open(sc: ScioContext): SCollection[T] = sc.objectFile[T](path)
}

final case class AvroTaps(self: Taps) {

  /** Get a `Future[Tap[T]]` of a Protobuf file. */
  def protobufFile[T: ClassTag: Coder](path: String)(implicit ev: T <:< Message): Future[Tap[T]] =
    self.mkTap(s"Protobuf: $path", () => self.isPathDone(path), () => ObjectFileTap[T](path))

  /** Get a `Future[Tap[T]]` of an object file. */
  def objectFile[T: ClassTag: Coder](path: String): Future[Tap[T]] =
    self.mkTap(s"Object file: $path", () => self.isPathDone(path), () => ObjectFileTap[T](path))

  /** Get a `Future[Tap[T]]` for an Avro file. */
  def avroFile[T: Coder](path: String, schema: Schema)(
    implicit ev: T <:< GenericRecord): Future[Tap[T]] =
    self.mkTap(s"Avro: $path", () => self.isPathDone(path), () => SchemaAvroTap[T](path, schema))

  /** Get a `Future[Tap[T]]` for typed Avro source. */
  def typedAvroFile[T <: HasAvroAnnotation: TypeTag: ClassTag: Coder](
    path: String): Future[Tap[T]] = {
    val avroT = AvroType[T]

    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val bcoder = Coder.avroGenericRecordCoder(avroT.schema)
    avroFile[GenericRecord](path, avroT.schema)
      .map(_.map(avroT.fromGenericRecord))
  }
}
