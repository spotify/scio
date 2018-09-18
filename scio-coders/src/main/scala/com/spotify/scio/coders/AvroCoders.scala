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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{AtomicCoder, AvroCoder, StringUtf8Coder}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver

private final class SlowGenericRecordCoder extends AtomicCoder[GenericRecord] {

  // TODO: can we find something more efficient than String ?
  private[this] val sc = StringUtf8Coder.of()

  override def encode(value: GenericRecord, os: OutputStream): Unit = {
    val schema = value.getSchema
    val coder = AvroCoder.of(schema)
    sc.encode(schema.toString, os)
    coder.encode(value, os)
  }

  override def decode(is: InputStream): GenericRecord = {
    val schemaStr = sc.decode(is)
    val schema = new Schema.Parser().parse(schemaStr)
    val coder = AvroCoder.of(schema)
    coder.decode(is)
  }

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(this,
                                        "Coder[GenericRecord] without schema is non-deterministic")
  override def consistentWithEquals(): Boolean = false
  override def structuralValue(value: GenericRecord): AnyRef =
    AvroCoder.of(value.getSchema).structuralValue(value)

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: GenericRecord): Boolean =
    AvroCoder.of(value.getSchema).isRegisterByteSizeObserverCheap(value)
  override def registerByteSizeObserver(value: GenericRecord,
                                        observer: ElementByteSizeObserver): Unit =
    AvroCoder.of(value.getSchema).registerByteSizeObserver(value, observer)
}

trait AvroCoders {
  import language.experimental.macros
  // TODO: Use a coder that does not serialize the schema
  def avroGenericRecordCoder(schema: Schema): Coder[GenericRecord] =
    Coder.beam(AvroCoder.of(schema))

  // XXX: similar to GenericAvroSerializer
  def avroGenericRecordCoder: Coder[GenericRecord] =
    Coder.beam(new SlowGenericRecordCoder)

  import org.apache.avro.specific.SpecificRecordBase
  implicit def genAvro[T <: SpecificRecordBase]: Coder[T] =
    macro com.spotify.scio.coders.CoderMacros.staticInvokeCoder[T]
}
