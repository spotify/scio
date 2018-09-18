/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.coders

import java.io.{InputStream, OutputStream}
import org.apache.beam.sdk.coders.{Coder => BCoder, _}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

final class AvroRawCoder[T] private (
  @transient var schema: org.apache.avro.Schema)
    extends AtomicCoder[T] {

  // makes the schema scerializable
  val schemaString = schema.toString

  @transient lazy val _schema =
    new org.apache.avro.Schema.Parser().parse(schemaString)

  @transient lazy val model = new org.apache.avro.specific.SpecificData()
  @transient lazy val encoder =
    new org.apache.avro.message.RawMessageEncoder[T](model, _schema)
  @transient lazy val decoder =
    new org.apache.avro.message.RawMessageDecoder[T](model, _schema)

  def encode(value: T, os: OutputStream): Unit =
    encoder.encode(value, os)

  def decode(is: InputStream): T =
    decoder.decode(is)
}

object AvroRawCoder {
  def apply[T](schema: org.apache.avro.Schema): AvroRawCoder[T] =
    new AvroRawCoder[T](schema)
}

private final class SlowGenericRecordCoder extends AtomicCoder[GenericRecord] {

  var coder: BCoder[GenericRecord] = _
  // TODO: can we find something more efficient than String ?
  val sc = StringUtf8Coder.of()

  def encode(value: GenericRecord, os: OutputStream): Unit = {
    val schema = value.getSchema
    if (coder == null) {
      coder = AvroCoder.of(schema)
    }
    sc.encode(schema.toString, os)
    coder.encode(value, os)
  }

  def decode(is: InputStream): GenericRecord = {
    val schemaStr = sc.decode(is)
    if (coder == null) {
      val schema = new Schema.Parser().parse(schemaStr)
      coder = AvroCoder.of(schema)
    }
    coder.decode(is)
  }
}

trait AvroCoders {
  import language.experimental.macros
  // TODO: Use a coder that does not serialize the schema
  def avroGenericRecordCoder(schema: Schema): Coder[GenericRecord] =
    Coder.beam(AvroRawCoder(schema))

  // XXX: similar to GenericAvroSerializer
  def avroGenericRecordCoder: Coder[GenericRecord] =
    Coder.beam(new SlowGenericRecordCoder)

  import org.apache.avro.specific.SpecificRecordBase
  implicit def genAvro[T <: SpecificRecordBase]: Coder[T] =
    macro com.spotify.scio.coders.CoderMacros.staticInvokeCoder[T]
}
