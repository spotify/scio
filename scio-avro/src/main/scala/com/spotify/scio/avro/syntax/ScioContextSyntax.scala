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

package com.spotify.scio.avro.syntax

import com.google.protobuf.Message
import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.avro._
import com.spotify.scio.avro.types.AvroType.HasAvroAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values._
import _root_.magnolify.protobuf.ProtobufType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Enhanced version of [[ScioContext]] with Avro methods. */
final class ScioContextOps(private val self: ScioContext) extends AnyVal {

  /**
   * Get an SCollection for an object file using default serialization.
   *
   * Serialized objects are stored in Avro files to leverage Avro's block file format. Note that
   * serialization is not guaranteed to be compatible across Scio releases.
   */
  def objectFile[T: Coder](
    path: String,
    suffix: String = ObjectFileIO.ReadParam.DefaultSuffix
  ): SCollection[T] =
    self.read(ObjectFileIO[T](path))(ObjectFileIO.ReadParam(suffix))

  def avroFile(path: String, schema: Schema): SCollection[GenericRecord] =
    avroFile(path, schema, GenericRecordIO.ReadParam.DefaultSuffix)

  // overloaded API. We can't use default params
  def avroFile(
    path: String,
    schema: Schema,
    suffix: String
  ): SCollection[GenericRecord] =
    avroFile(path, schema, suffix, GenericRecordIO.ReadParam.DefaultDatumFactory)

  // overloaded API. We can't use default params
  def avroFile(
    path: String,
    schema: Schema,
    datumFactory: AvroDatumFactory[GenericRecord]
  ): SCollection[GenericRecord] =
    avroFile(path, schema, GenericRecordIO.ReadParam.DefaultSuffix, datumFactory)

  // overloaded API. We can't use default params
  def avroFile(
    path: String,
    schema: Schema,
    suffix: String,
    datumFactory: AvroDatumFactory[GenericRecord]
  ): SCollection[GenericRecord] = {
    val param = GenericRecordIO.ReadParam(suffix, datumFactory)
    self.read(GenericRecordIO(path, schema))(param)
  }

  /**
   * Get an SCollection of type [[T]] for data stored in Avro format after applying parseFn to map a
   * serialized [[org.apache.avro.generic.GenericRecord GenericRecord]] to type [[T]].
   *
   * This API should be used with caution as the `parseFn` reads from a `GenericRecord` and hence is
   * not type checked.
   *
   * This is intended to be used when attempting to read `GenericRecord`s without specifying a
   * schema (hence the writer schema is used to deserialize) and then directly converting to a type
   * [[T]] using a `parseFn`. This avoids creation of an intermediate `SCollection[GenericRecord]`
   * which can be in efficient because `Coder[GenericRecord]` is inefficient without a known Avro
   * schema.
   *
   * Example usage: This code reads Avro fields "id" and "name" and de-serializes only those two
   * into CaseClass
   *
   * {{{
   *   val sColl: SCollection[CaseClass] =
   *     sc.parseAvroFile("gs://.....") { g =>
   *       CaseClass(g.get("id").asInstanceOf[Int], g.get("name").asInstanceOf[String])
   *     }
   * }}}
   */
  @experimental
  def parseAvroFile[T: Coder](
    path: String,
    suffix: String = GenericRecordParseIO.ReadParam.DefaultSuffix,
    datumFactory: AvroDatumFactory[GenericRecord] =
      GenericRecordParseIO.ReadParam.DefaultDatumFactory
  )(
    parseFn: GenericRecord => T
  ): SCollection[T] = {
    val param = GenericRecordParseIO.ReadParam(suffix, datumFactory)
    self.read(GenericRecordParseIO[T](path, parseFn))(param)
  }

  /**
   * Get an SCollection of type [[org.apache.avro.specific.SpecificRecord SpecificRecord]] for an
   * Avro file.
   */
  def avroFile[T <: SpecificRecord: ClassTag](path: String): SCollection[T] =
    avroFile(path, SpecificRecordIO.ReadParam.DefaultSuffix)

  // overloaded API. We can't use default params
  /**
   * Get an SCollection of type [[org.apache.avro.specific.SpecificRecord SpecificRecord]] for an
   * Avro file.
   */
  def avroFile[T <: SpecificRecord: ClassTag](path: String, suffix: String): SCollection[T] =
    avroFile(path, suffix, SpecificRecordIO.ReadParam.DefaultDatumFactory)

  // overloaded API. We can't use default params
  /**
   * Get an SCollection of type [[org.apache.avro.specific.SpecificRecord SpecificRecord]] for an
   * Avro file.
   */
  def avroFile[T <: SpecificRecord: ClassTag](
    path: String,
    datumFactory: AvroDatumFactory[T]
  ): SCollection[T] =
    avroFile(path, SpecificRecordIO.ReadParam.DefaultSuffix, datumFactory)

  // overloaded API. We can't use default params
  /**
   * Get an SCollection of type [[org.apache.avro.specific.SpecificRecord SpecificRecord]] for an
   * Avro file.
   */
  def avroFile[T <: SpecificRecord: ClassTag](
    path: String,
    suffix: String,
    datumFactory: AvroDatumFactory[T]
  ): SCollection[T] = {
    val param = SpecificRecordIO.ReadParam(suffix, datumFactory)
    self.read(SpecificRecordIO[T](path))(param)
  }

  /**
   * Get a typed SCollection from an Avro schema.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.avro.types.AvroType AvroType.fromSchema]],
   * [[com.spotify.scio.avro.types.AvroType AvroType.fromPath]], or
   * [[com.spotify.scio.avro.types.AvroType AvroType.toSchema]].
   */
  def typedAvroFile[T <: HasAvroAnnotation: TypeTag: Coder](
    path: String,
    suffix: String = AvroTypedIO.ReadParam.DefaultSuffix
  ): SCollection[T] =
    self.read(AvroTypedIO[T](path))(AvroTypedIO.ReadParam(suffix))

  /**
   * Get an SCollection for a Protobuf file.
   *
   * Protobuf messages are serialized into `Array[Byte]` and stored in Avro files to leverage Avro's
   * block file format.
   */
  def protobufFile[T <: Message: ClassTag](
    path: String,
    suffix: String = ProtobufObjectFileIO.ReadParam.DefaultSuffix
  ): SCollection[T] =
    self.read(ProtobufObjectFileIO[T](path))(ProtobufObjectFileIO.ReadParam(suffix))

  /**
   * Read back protobuf messages serialized to `Array[Byte]` and stored in Avro files then map them
   * automatically to type `T` via the implicit [[magnolify.protobuf.ProtobufType]]
   */
  def typedProtobufFile[T: Coder, U <: Message: ClassTag](
    path: String,
    suffix: String = ProtobufObjectFileIO.ReadParam.DefaultSuffix
  )(implicit pt: ProtobufType[T, U]): SCollection[T] =
    self.read(TypedProtobufObjectFileIO[T, U](path, pt))(ProtobufObjectFileIO.ReadParam(suffix))
}

/** Enhanced with Avro methods. */
trait ScioContextSyntax {
  implicit def avroScioContextOps(c: ScioContext): ScioContextOps = new ScioContextOps(c)
}
