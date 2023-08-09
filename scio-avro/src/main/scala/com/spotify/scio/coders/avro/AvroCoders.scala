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

package com.spotify.scio.coders.avro

import com.spotify.scio.avro.{GenericRecordDatumFactory, SpecificRecordDatumFactory}
import com.spotify.scio.coders.Coder
import com.spotify.scio.util.ScioUtil
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.avro.specific.{SpecificData, SpecificFixed, SpecificRecord}
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{AtomicCoder, Coder => BCoder, CustomCoder, StringUtf8Coder}
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory
import org.apache.beam.sdk.util.common.ElementByteSizeObserver

import java.io.{InputStream, OutputStream}
import scala.reflect.ClassTag

final private class SlowGenericRecordCoder extends AtomicCoder[GenericRecord] {
  // TODO: can we find something more efficient than String ?
  private val sc = StringUtf8Coder.of()
  @transient private lazy val parser = new Schema.Parser()

  private def genericCoder(schema: Schema): BCoder[GenericRecord] =
    AvroCoder.of(GenericRecordDatumFactory, schema)

  override def encode(value: GenericRecord, os: OutputStream): Unit = {
    val schema = value.getSchema
    val coder = genericCoder(schema)
    sc.encode(schema.toString, os)
    coder.encode(value, os)
  }

  override def decode(is: InputStream): GenericRecord = {
    val schemaStr = sc.decode(is)
    val schema = parser.parse(schemaStr)
    val coder = genericCoder(schema)
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
    genericCoder(value.getSchema).structuralValue(value)

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: GenericRecord): Boolean =
    genericCoder(value.getSchema).isRegisterByteSizeObserverCheap(value)
  override def registerByteSizeObserver(
    value: GenericRecord,
    observer: ElementByteSizeObserver
  ): Unit =
    genericCoder(value.getSchema).registerByteSizeObserver(value, observer)
}

/**
 * Implementation is legit only for SpecificFixed, not GenericFixed
 * @see
 *   [[org.apache.beam.sdk.extensions.avro.coders.AvroCoder]]
 */
final private class SpecificFixedCoder[A <: SpecificFixed](cls: Class[A]) extends CustomCoder[A] {
  // lazy because AVRO Schema isn't serializable
  @transient private lazy val schema: Schema = SpecificData.get().getSchema(cls)
  private val size = schema.getFixedSize

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

  override def verifyDeterministic(): Unit = {}
}

private object SpecificFixedCoder {
  def apply[A <: SpecificFixed: ClassTag]: Coder[A] = {
    val cls = ScioUtil.classOf[A]
    Coder.beam(new SpecificFixedCoder[A](cls))
  }
}

trait AvroCoders {

  /**
   * Create a Coder for Avro GenericRecord given the schema of the GenericRecord.
   *
   * @param schema
   *   AvroSchema for the Coder.
   * @return
   *   Coder[GenericRecord]
   */
  def avroGenericRecordCoder(schema: Schema): Coder[GenericRecord] =
    avroCoder(GenericRecordDatumFactory, schema)

  // XXX: similar to GenericAvroSerializer
  def avroGenericRecordCoder: Coder[GenericRecord] =
    Coder.beam(new SlowGenericRecordCoder)

  implicit def avroSpecificRecordCoder[T <: SpecificRecord: ClassTag]: Coder[T] = {
    val recordClass = ScioUtil.classOf[T]
    val factory = new SpecificRecordDatumFactory(recordClass)
    val schema = SpecificData.get().getSchema(recordClass)
    avroCoder(factory, schema)
  }

  def avroCoder[T <: IndexedRecord](factory: AvroDatumFactory[T], schema: Schema): Coder[T] =
    Coder.beam(AvroCoder.of(factory, schema))

  implicit def avroSpecificFixedCoder[T <: SpecificFixed: ClassTag]: Coder[T] =
    SpecificFixedCoder[T]
}
