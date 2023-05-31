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
  @transient private val schema: Schema,
  params: AvroIO.ReadParam
) extends Tap[GenericRecord] {
  private lazy val s = Externalizer(schema)

  override def value: Iterator[GenericRecord] =
    FileStorage(path, params.suffix).avroFile[GenericRecord](s.get)

  override def open(sc: ScioContext): SCollection[GenericRecord] =
    sc.read(GenericRecordIO(path, s.get))(params)
}

/** Tap for [[org.apache.avro.specific.SpecificRecord SpecificRecord]] Avro files. */
final case class SpecificRecordTap[T <: SpecificRecord: ClassTag: Coder](
  path: String,
  params: AvroIO.ReadParam
) extends Tap[T] {
  override def value: Iterator[T] =
    FileStorage(path, params.suffix).avroFile[T]()

  override def open(sc: ScioContext): SCollection[T] =
    sc.read(SpecificRecordIO[T](path))(params)
}

/**
 * Tap for reading [[org.apache.avro.generic.GenericRecord GenericRecord]] Avro files and applying a
 * parseFn to parse it to the given type [[T]]
 */
final case class GenericRecordParseTap[T: Coder](
  path: String,
  parseFn: GenericRecord => T,
  params: AvroIO.ReadParam
) extends Tap[T] {
  override def value: Iterator[T] =
    FileStorage(path, params.suffix)
      // Read Avro GenericRecords, with the writer specified schema
      .avroFile[GenericRecord](schema = null)
      .map(parseFn)

  override def open(sc: ScioContext): SCollection[T] =
    sc.read(GenericRecordParseIO[T](path, parseFn))(params)
}

/**
 * Tap for object files. Note that serialization is not guaranteed to be compatible across Scio
 * releases.
 */
final case class ObjectFileTap[T: Coder](
  path: String,
  params: AvroIO.ReadParam
) extends Tap[T] {
  override def value: Iterator[T] = {
    val elemCoder = CoderMaterializer.beamWithDefault(Coder[T])
    FileStorage(path, params.suffix)
      .avroFile[GenericRecord](AvroBytesUtil.schema)
      .map(r => AvroBytesUtil.decode(elemCoder, r))
  }
  override def open(sc: ScioContext): SCollection[T] =
    sc.read(ObjectFileIO[T](path))(params)
}

final case class AvroTaps(self: Taps) {

  /** Get a `Future[Tap[T]]` of a Protobuf file. */
  def protobufFile[T: Coder](path: String, params: ProtobufIO.ReadParam = ProtobufIO.ReadParam())(
    implicit ev: T <:< Message
  ): Future[Tap[T]] =
    self.mkTap(
      s"Protobuf: $path",
      () => self.isPathDone(path, params.suffix),
      () => ObjectFileTap[T](path, params)
    )

  /** Get a `Future[Tap[T]]` of an object file. */
  def objectFile[T: Coder](
    path: String,
    params: ObjectFileIO.ReadParam = ObjectFileIO.ReadParam()
  ): Future[Tap[T]] =
    self.mkTap(
      s"Object file: $path",
      () => self.isPathDone(path, params.suffix),
      () => ObjectFileTap[T](path, params)
    )

  /**
   * Get a `Future[Tap[T]]` for [[org.apache.avro.generic.GenericRecord GenericRecord]] Avro file.
   */
  def avroFile(path: String, schema: Schema): Future[Tap[GenericRecord]] =
    avroFile(path, schema, AvroIO.ReadParam())

  // overloaded API. We can't use default params
  /**
   * Get a `Future[Tap[T]]` for [[org.apache.avro.generic.GenericRecord GenericRecord]] Avro file.
   */
  def avroFile(path: String, schema: Schema, params: AvroIO.ReadParam): Future[Tap[GenericRecord]] =
    self.mkTap(
      s"Avro: $path",
      () => self.isPathDone(path, params.suffix),
      () => GenericRecordTap(path, schema, params)
    )

  /**
   * Get a `Future[Tap[T]]` for [[org.apache.avro.specific.SpecificRecord SpecificRecord]] Avro
   * file.
   */
  def avroFile[T <: SpecificRecord: ClassTag: Coder](path: String): Future[Tap[T]] =
    avroFile[T](path, AvroIO.ReadParam())

  // overloaded API. We can't use default params
  /**
   * Get a `Future[Tap[T]]` for [[org.apache.avro.specific.SpecificRecord SpecificRecord]] Avro
   * file.
   */
  def avroFile[T <: SpecificRecord: ClassTag: Coder](
    path: String,
    params: AvroIO.ReadParam
  ): Future[Tap[T]] =
    self.mkTap(
      s"Avro: $path",
      () => self.isPathDone(path, params.suffix),
      () => SpecificRecordTap[T](path, params)
    )

  /** Get a `Future[Tap[T]]` for typed Avro source. */
  def typedAvroFile[T <: HasAvroAnnotation: TypeTag: Coder](
    path: String,
    params: AvroIO.ReadParam = AvroIO.ReadParam()
  ): Future[Tap[T]] = {
    val avroT = AvroType[T]

    import scala.concurrent.ExecutionContext.Implicits.global
    avroFile(path, avroT.schema, params).map(_.map(avroT.fromGenericRecord))
  }
}
