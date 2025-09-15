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
import com.spotify.scio.avro.io.AvroFileStorage
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.io.{Tap, Taps}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values._
import com.twitter.chill.Externalizer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.{SpecificData, SpecificRecord}

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Tap for [[org.apache.avro.generic.GenericRecord GenericRecord]] Avro files. */
final case class GenericRecordTap(
  path: String,
  @transient private val schema: Schema,
  params: GenericRecordIO.ReadParam
) extends Tap[GenericRecord] {
  private lazy val s = Externalizer(schema)

  override def value: Iterator[GenericRecord] = {
    val datumFactory = Option(params.datumFactory).getOrElse(GenericRecordDatumFactory)
    AvroFileStorage(path, params.suffix).avroFile(datumFactory(s.get, s.get))
  }

  override def open(sc: ScioContext): SCollection[GenericRecord] =
    sc.read(GenericRecordIO(path, s.get))(params)
}

/** Tap for [[org.apache.avro.specific.SpecificRecord SpecificRecord]] Avro files. */
final case class SpecificRecordTap[T <: SpecificRecord: ClassTag](
  path: String,
  params: SpecificRecordIO.ReadParam[T]
) extends Tap[T] {

  @transient private lazy val recordClass: Class[T] = ScioUtil.classOf[T]
  @transient private lazy val schema: Schema = SpecificData.get().getSchema(recordClass)

  override def value: Iterator[T] = {
    val datumFactory =
      Option(params.datumFactory).getOrElse(new SpecificRecordDatumFactory[T](recordClass))
    AvroFileStorage(path, params.suffix).avroFile[T](datumFactory(schema, schema))
  }

  override def open(sc: ScioContext): SCollection[T] =
    sc.read(SpecificRecordIO[T](path))(params)
}

object ObjectFileTap {
  def apply[T: Coder](path: String, params: ObjectFileIO.ReadParam): Tap[T] = {
    val objectCoder = CoderMaterializer.beamWithDefault(Coder[T])
    GenericRecordTap(path, AvroBytesUtil.schema, params)
      .map(record => AvroBytesUtil.decode(objectCoder, record))
  }
}

object ProtobufFileTap {
  def apply[T <: Message: ClassTag](path: String, params: ProtobufIO.ReadParam): Tap[T] =
    ObjectFileTap(path, params)(Coder.protoMessageCoder[T])
}

final case class AvroTaps(self: Taps) {

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

  /** Get a `Future[Tap[T]]` of a Protobuf file. */
  def protobufFile[T <: Message: ClassTag](
    path: String,
    params: ProtobufIO.ReadParam = ProtobufIO.ReadParam()
  ): Future[Tap[T]] =
    self.mkTap(
      s"Protobuf: $path",
      () => self.isPathDone(path, params.suffix),
      () => ProtobufFileTap[T](path, params)
    )

  /** Get a `Future[Tap[T]]` for [[org.apache.avro.generic.GenericRecord GenericRecord]] Avro file. */
  def avroFile(path: String, schema: Schema): Future[Tap[GenericRecord]] =
    avroFile(path, schema, GenericRecordIO.ReadParam())

  // overloaded API. We can't use default params
  /** Get a `Future[Tap[T]]` for [[org.apache.avro.generic.GenericRecord GenericRecord]] Avro file. */
  def avroFile(
    path: String,
    schema: Schema,
    params: GenericRecordIO.ReadParam
  ): Future[Tap[GenericRecord]] =
    self.mkTap(
      s"Avro: $path",
      () => self.isPathDone(path, params.suffix),
      () => GenericRecordTap(path, schema, params)
    )

  /**
   * Get a `Future[Tap[T]]` for [[org.apache.avro.specific.SpecificRecord SpecificRecord]] Avro
   * file.
   */
  def avroFile[T <: SpecificRecord: ClassTag](path: String): Future[Tap[T]] =
    avroFile[T](path, SpecificRecordIO.ReadParam[T]())

  // overloaded API. We can't use default params
  /**
   * Get a `Future[Tap[T]]` for [[org.apache.avro.specific.SpecificRecord SpecificRecord]] Avro
   * file.
   */
  def avroFile[T <: SpecificRecord: ClassTag](
    path: String,
    params: SpecificRecordIO.ReadParam[T]
  ): Future[Tap[T]] =
    self.mkTap(
      s"Avro: $path",
      () => self.isPathDone(path, params.suffix),
      () => SpecificRecordTap[T](path, params)
    )

  /** Get a `Future[Tap[T]]` for typed Avro source. */
  def typedAvroFile[T <: HasAvroAnnotation: TypeTag: Coder](
    path: String,
    params: AvroTypedIO.ReadParam = AvroTypedIO.ReadParam()
  ): Future[Tap[T]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val avroT = AvroType[T]
    avroFile(path, avroT.schema, params).map(_.map(avroT.fromGenericRecord))
  }
}
