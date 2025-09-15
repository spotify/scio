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

package com.spotify.scio.coders

import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.{DatumWriter, Encoder}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.extensions.avro.io.AvroSink.DatumWriterFactory
import org.apache.beam.sdk.util.CoderUtils

import java.nio.ByteBuffer

private[scio] object AvroBytesUtil {
  val schema: Schema = SchemaBuilder
    .record("AvroBytesRecord")
    .fields()
    .requiredBytes("bytes")
    .endRecord()

  private val byteField = schema.getField("bytes")

  def datumWriterFactory[T: Coder]: DatumWriterFactory[T] = {
    val bCoder = CoderMaterializer.beamWithDefault(Coder[T])
    (schema: Schema) =>
      new DatumWriter[T] {
        private val underlying = new GenericDatumWriter[GenericRecord](schema)

        override def setSchema(schema: Schema): Unit = underlying.setSchema(schema)

        override def write(datum: T, out: Encoder): Unit =
          underlying.write(AvroBytesUtil.encode(bCoder, datum), out)
      }
  }

  def encode[T](coder: BCoder[T], obj: T): GenericRecord = {
    val bytes = CoderUtils.encodeToByteArray(coder, obj)
    new GenericRecordBuilder(schema)
      .set(byteField, ByteBuffer.wrap(bytes))
      .build()
  }

  def decode[T](coder: BCoder[T], record: GenericRecord): T = {
    val bb = record.get(byteField.pos()).asInstanceOf[ByteBuffer]
    val bytes = java.util.Arrays.copyOfRange(bb.array(), bb.position(), bb.limit())
    CoderUtils.decodeFromByteArray(coder, bytes)
  }
}
