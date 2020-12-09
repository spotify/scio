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
import com.spotify.scio._
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.io.{FileStorage, Tap, Taps}
import com.spotify.scio.values._
import com.twitter.chill.Externalizer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Tap for [[org.apache.avro.generic.GenericRecord GenericRecord]] Avro files. */
final case class GenericRecordTap(
  path: String,
  @transient private val
  schema: Schema
) extends Tap[GenericRecord] {
  private lazy val s = Externalizer(schema)

  override def value: Iterator[GenericRecord] = FileStorage(path).avroFile[GenericRecord](s.get)

  override def open(sc: ScioContext): SCollection[GenericRecord] = {
    implicit val coder = Coder.avroGenericRecordCoder(s.get)
    sc.read(GenericRecordIO(path, s.get))
  }
}

/** Tap for [[org.apache.avro.specific.SpecificRecord SpecificRecord]] Avro files. */
final case class SpecificRecordTap[T <: SpecificRecord: ClassTag: Coder](path: String)
    extends Tap[T] {
  override def value: Iterator[T] = FileStorage(path).avroFile[T]()

  override def open(sc: ScioContext): SCollection[T] = sc.avroFile[T](path)
}

/**
 * Tap for reading [[org.apache.avro.generic.GenericRecord GenericRecord]] Avro files and applying
 * a parseFn to parse it to the given type [[T]]
 */
final case class GenericRecordParseTap[T: Coder](
  path: String,
  parseFn: GenericRecord => T
) extends Tap[T] {
  override def value: Iterator[T] =
    FileStorage(path)
      // Read Avro GenericRecords, with the writer specified schema
      .avroFile[GenericRecord](schema = null)
      .map(parseFn)

  override def open(sc: ScioContext): SCollection[T] = sc.parseAvroFile(path)(parseFn)
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

  /**
   * Get a `Future[Tap[T]]` for [[org.apache.avro.generic.GenericRecord GenericRecord]] Avro
   * file.
   */
  def avroFile(path: String, schema: Schema): Future[Tap[GenericRecord]] =
    self.mkTap(
      s"Avro: $path",
      () => self.isPathDone(path),
      () => GenericRecordTap(path, schema)
    )

  /**
   * Get a `Future[Tap[T]]` for
   * [[org.apache.avro.specific.SpecificRecord SpecificRecord]] Avro file.
   */
  def avroFile[T <: SpecificRecord: ClassTag: Coder](path: String): Future[Tap[T]] =
    self.mkTap(s"Avro: $path", () => self.isPathDone(path), () => SpecificRecordTap[T](path))

  /** Get a `Future[Tap[T]]` for typed Avro source. */
  def typedAvroFile[T <: HasAvroAnnotation: TypeTag: ClassTag: Coder](
    path: String
  ): Future[Tap[T]] = {
    val avroT = AvroType[T]

    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val bcoder: Coder[GenericRecord] = Coder.avroGenericRecordCoder(avroT.schema)
    avroFile(path, avroT.schema)
      .map(_.map(avroT.fromGenericRecord))
  }
}
