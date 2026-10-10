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
import java.util.ServiceLoader

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.util.ClassSecurityValidator
import com.spotify.scio.util.AvroGeneratedTrustInitializer
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

  it should "read String instead of Utf8 with the fast reader on" in {
    val factory = new SpecificRecordDatumFactory(classOf[StringFieldTest])
    val schema = StringFieldTest.getClassSchema
    val record = StringFieldTest
      .newBuilder()
      .setStrField("s")
      .setMapField(Map[CharSequence, CharSequence]("k" -> "v").asJava)
      .setArrayField(List[CharSequence]("a").asJava)
      .build()

    val reader = factory(schema, schema)
    reader.asInstanceOf[SpecificDatumReader[_]].getData.isFastReaderEnabled shouldBe true
    val read = reader.read(null, decoder(factory(schema).write(record, _)))
    read.getStrField shouldBe a[String]
    read.getMapField.asScala.toList
      .flatMap { case (k, v) => List(k, v) }
      .foreach(_ shouldBe a[String])
    read.getArrayField.asScala.foreach(_ shouldBe a[String])
    read shouldBe record
  }

  "AvroGeneratedTrustInitializer" should "be registered with ServiceLoader" in {
    val loaded = ServiceLoader.load(classOf[JvmInitializer]).iterator().asScala.map(_.getClass)
    loaded.toList should contain(classOf[AvroGeneratedTrustInitializer])
  }

  "GenericRecordDatumFactory" should "read String instead of Utf8 and keep the schema" in {
    val schema: Schema = SchemaBuilder
      .record("R")
      .fields()
      .requiredString("s")
      .name("m")
      .`type`()
      .map()
      .values()
      .stringType()
      .noDefault()
      .name("a")
      .`type`()
      .array()
      .items()
      .stringType()
      .noDefault()
      .endRecord()
    val record = new GenericData.Record(schema)
    record.put("s", "value")
    record.put("m", Map("k" -> "v").asJava)
    record.put("a", List("x").asJava)

    GenericData.get().isFastReaderEnabled shouldBe true
    val read = GenericRecordDatumFactory(schema, schema)
      .read(null, decoder(new GenericDatumWriter[GenericRecord](schema).write(record, _)))
    read.get("s") shouldBe a[String]
    read.get("s") shouldBe "value"
    read
      .get("m")
      .asInstanceOf[java.util.Map[_, _]]
      .asScala
      .toList
      .flatMap { case (k, v) => List(k, v) }
      .foreach(_ shouldBe a[String])
    read.get("a").asInstanceOf[java.util.List[_]].asScala.foreach(_ shouldBe a[String])
    read.getSchema shouldBe theSameInstanceAs(schema)
    read shouldBe record
    // the shared singleton is left alone
    GenericData.get().isFastReaderEnabled shouldBe true
  }

  private def decoder(write: BinaryEncoder => Unit): BinaryDecoder = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    write(encoder)
    encoder.flush()
    DecoderFactory.get().binaryDecoder(out.toByteArray, null)
  }

}
