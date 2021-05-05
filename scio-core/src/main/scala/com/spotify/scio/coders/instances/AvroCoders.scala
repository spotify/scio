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

package com.spotify.scio.coders.instances

import java.io.{InputStream, OutputStream}

import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.{SpecificData, SpecificFixed}
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{AtomicCoder, AvroCoder, StringUtf8Coder}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver

import scala.reflect.{classTag, ClassTag}

final private class SlowGenericRecordCoder extends AtomicCoder[GenericRecord] {
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
    throw new NonDeterministicException(
      this,
      "Coder[GenericRecord] without schema is non-deterministic"
    )
  override def consistentWithEquals(): Boolean = false
  override def structuralValue(value: GenericRecord): AnyRef =
    AvroCoder.of(value.getSchema).structuralValue(value)

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: GenericRecord): Boolean =
    AvroCoder.of(value.getSchema).isRegisterByteSizeObserverCheap(value)
  override def registerByteSizeObserver(
    value: GenericRecord,
    observer: ElementByteSizeObserver
  ): Unit =
    AvroCoder.of(value.getSchema).registerByteSizeObserver(value, observer)
}

/**
 * Implementation is legit only for SpecificFixed, not GenericFixed
 * @see [[org.apache.beam.sdk.coders.AvroCoder]]
 */
final private class SpecificFixedCoder[A <: SpecificFixed](cls: Class[A]) extends AtomicCoder[A] {
  // lazy because AVRO Schema isn't serializable
  @transient private[this] lazy val schema: Schema = SpecificData.get().getSchema(cls)
  private[this] val size = SpecificData.get().getSchema(cls).getFixedSize

  def encode(value: A, outStream: OutputStream): Unit = {
    assert(value.bytes().length == size)
    outStream.write(value.bytes())
  }

  def decode(inStream: InputStream): A = {
    val bytes = new Array[Byte](size)
    inStream.read(bytes)
    val old = SpecificData.newInstance(cls, schema)
    SpecificData.get().createFixed(old, bytes, schema).asInstanceOf[A]
  }

  override def isRegisterByteSizeObserverCheap(value: A): Boolean = true

  override def getEncodedElementByteSize(value: A): Long = size.toLong

  override def consistentWithEquals(): Boolean = true

  override def structuralValue(value: A): AnyRef = value
}

private object SpecificFixedCoder {
  def apply[A <: SpecificFixed: ClassTag]: Coder[A] = {
    val cls = classTag[A].runtimeClass.asInstanceOf[Class[A]]
    Coder.beam(new SpecificFixedCoder[A](cls))
  }
}

trait AvroCoders extends AvroCodersMacros {

  /**
   * Create a Coder for Avro GenericRecord given the schema of the GenericRecord.
   *
   * @param schema AvroSchema for the Coder.
   * @return Coder[GenericRecord]
   */
  // TODO: Use a coder that does not serialize the schema
  def avroGenericRecordCoder(schema: Schema): Coder[GenericRecord] =
    Coder.beam(AvroCoder.of(schema))

  // XXX: similar to GenericAvroSerializer
  def avroGenericRecordCoder: Coder[GenericRecord] =
    Coder.beam(new SlowGenericRecordCoder)

  implicit def avroSpecificFixedCoder[T <: SpecificFixed: ClassTag]: Coder[T] =
    SpecificFixedCoder[T]
}
