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
import com.spotify.scio.coders.{Coder, CoderGrammar}
import com.spotify.scio.util.ScioUtil
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificData, SpecificFixed, SpecificRecord}
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{CustomCoder, StringUtf8Coder}
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory
import org.apache.beam.sdk.util.EmptyOnDeserializationThreadLocal

import java.io.{InputStream, OutputStream}
import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

final private class SlowGenericRecordCoder extends CustomCoder[GenericRecord] {
  // Schema is serializable on avro 1.9+
  // Use String for 1.8 compat
  private val sc = StringUtf8Coder.of()

  /**
   * Reuse parsed schemas because GenericDatumReader caches decoders based on schema reference
   * equality.
   * @see
   *   [[org.apache.avro.generic.GenericDatumReader.getResolver]].
   */
  @transient private lazy val schemaCache = new TrieMap[String, Schema]()

  private val encoder = new EmptyOnDeserializationThreadLocal[BinaryEncoder]()
  private val decoder = new EmptyOnDeserializationThreadLocal[BinaryDecoder]()

  override def encode(value: GenericRecord, os: OutputStream): Unit = {
    val enc = EncoderFactory.get().directBinaryEncoder(os, encoder.get())
    encoder.set(enc)
    val schema = value.getSchema
    val schemaString = schema.toString
    sc.encode(schemaString, os)
    val writer = AvroDatumFactory.generic()(schema)
    writer.write(value, enc)
  }

  override def decode(is: InputStream): GenericRecord = {
    val dec = DecoderFactory.get().directBinaryDecoder(is, decoder.get())
    decoder.set(dec)
    val schemaString = sc.decode(is)
    val schema = schemaCache.getOrElseUpdate(schemaString, new Schema.Parser().parse(schemaString))
    val reader = AvroDatumFactory.generic()(schema, schema)
    reader.read(null, dec)
  }

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this,
      "Coder[GenericRecord] without schema is non-deterministic"
    )
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

trait AvroCoders extends CoderGrammar {

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
    beam(new SlowGenericRecordCoder)

  implicit def avroSpecificRecordCoder[T <: SpecificRecord: ClassTag]: Coder[T] = {
    val recordClass = ScioUtil.classOf[T]
    val factory = new SpecificRecordDatumFactory(recordClass)
    val schema = SpecificData.get().getSchema(recordClass)
    avroCoder(factory, schema)
  }

  def avroCoder[T <: IndexedRecord](factory: AvroDatumFactory[T], schema: Schema): Coder[T] =
    beam(AvroCoder.of(factory, schema))

  implicit def avroSpecificFixedCoder[T <: SpecificFixed: ClassTag]: Coder[T] =
    SpecificFixedCoder[T]
}

private[coders] object AvroCoders extends AvroCoders
