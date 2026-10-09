/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.avro

import java.io.ByteArrayOutputStream
import java.time.{Instant, LocalDate, LocalTime}
import java.util.ServiceLoader

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.util.ClassSecurityValidator
import org.apache.beam.sdk.harness.JvmInitializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class AvroDatumFactoryTest extends AnyFlatSpec with Matchers {

  "SpecificRecordDatumFactory" should "load model with conversions" in {
    val factory = new SpecificRecordDatumFactory(classOf[LogicalTypesTest])
    val schema = LogicalTypesTest.getClassSchema

    {
      val writer = factory(schema)
      val data = writer.asInstanceOf[SpecificDatumWriter[LogicalTypesTest]].getData
      // top-level
      val timestamp = data.getConversionFor(LogicalTypes.timestampMillis())
      timestamp shouldBe a[TimeConversions.TimestampMillisConversion]
      // nested-level
      val date = data.getConversionFor(LogicalTypes.date())
      date shouldBe a[TimeConversions.DateConversion]
      val time = data.getConversionFor(LogicalTypes.timeMillis())
      time shouldBe a[TimeConversions.TimeMillisConversion]
    }

    {
      val reader = factory(schema, schema)
      val data = reader.asInstanceOf[SpecificDatumReader[LogicalTypesTest]].getData
      // top-level
      val timestamp = data.getConversionFor(LogicalTypes.timestampMillis())
      timestamp shouldBe a[TimeConversions.TimestampMillisConversion]
      // nested-level
      val date = data.getConversionFor(LogicalTypes.date())
      date shouldBe a[TimeConversions.DateConversion]
      val time = data.getConversionFor(LogicalTypes.timeMillis())
      time shouldBe a[TimeConversions.TimeMillisConversion]
    }
  }

  it should "allow classes with 'conversions' field" in {
    val f = new SpecificRecordDatumFactory(classOf[NameConflict])
    val schema = LogicalTypesTest.getClassSchema
    noException shouldBe thrownBy(f(schema))
    noException shouldBe thrownBy(f(schema, schema))
  }

  it should "trust avro generated classes" in {
    new SpecificRecordDatumFactory(classOf[LogicalTypesTest])(
      LogicalTypesTest.getClassSchema,
      LogicalTypesTest.getClassSchema
    )
    val validator = ClassSecurityValidator.getGlobal
    validator.toString should startWith("AvroGenerated or ")
    validator.isTrusted(classOf[LogicalTypesTest]) shouldBe true
    validator.isTrusted(classOf[LocalDateTimeTest]) shouldBe true
    validator.isTrusted(classOf[AvroDatumFactoryTest]) shouldBe false
  }

  it should "read records and disable the fast reader" in {
    val factory = new SpecificRecordDatumFactory(classOf[LogicalTypesTest])
    val schema = LogicalTypesTest.getClassSchema
    val record = LogicalTypesTest
      .newBuilder()
      .setTimestamp(Instant.ofEpochMilli(1000))
      .setLocalDateTime(new LocalDateTimeTest(LocalDate.of(2026, 1, 1), LocalTime.NOON))
      .build()

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    factory(schema).write(record, encoder)
    encoder.flush()

    val reader = factory(schema, schema)
    reader.asInstanceOf[SpecificDatumReader[_]].getData.isFastReaderEnabled shouldBe false
    reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray, null)) shouldBe record
  }

  "AvroCompatInitializer" should "be registered with ServiceLoader" in {
    val loaded = ServiceLoader.load(classOf[JvmInitializer]).iterator().asScala.map(_.getClass)
    loaded.toList should contain(classOf[AvroCompatInitializer])
  }

  "GenericRecordDatumFactory" should "read String instead of Utf8 with the fast reader on by default" in {
    val schema: Schema = SchemaBuilder.record("R").fields().requiredString("s").endRecord()
    val record = new GenericData.Record(schema)
    record.put("s", "value")

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    new GenericDatumWriter[GenericRecord](schema).write(record, encoder)
    encoder.flush()

    GenericData.get().isFastReaderEnabled shouldBe true
    val reader = GenericRecordDatumFactory(schema, schema)
    val read = reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray, null))
    read.get("s") shouldBe a[String]
    read.get("s") shouldBe "value"
    // the shared singletons are left alone
    GenericData.get().isFastReaderEnabled shouldBe true
  }

}
