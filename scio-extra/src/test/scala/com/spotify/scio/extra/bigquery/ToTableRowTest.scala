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

package com.spotify.scio.extra.bigquery

import java.math.{BigDecimal => JBigDecimal}
import java.nio.ByteBuffer

import com.google.protobuf.ByteString
import com.spotify.scio.bigquery.TableRow
import org.apache.avro.generic.GenericData
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding
import org.joda.time.{DateTime, LocalDate, LocalTime}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ToTableRowTest extends AnyFlatSpec with Matchers {
  val expectedOutput: TableRow = new TableRow()
    .set("booleanField", true)
    .set("intField", 1)
    .set("stringField", "someString")
    .set("longField", 1L)
    .set("doubleField", 1.0)
    .set("floatField", 1f)
    .set("bytesField", BaseEncoding.base64Url().encode("someBytes".getBytes))
    .set("unionField", "someUnion")
    .set("arrayField", List(new TableRow().set("nestedField", "nestedValue")).asJava)
    .set("mapField", List(new TableRow().set("key", "mapKey").set("value", 1.0d)).asJava)
    .set("enumField", Kind.FOO.toString)
    .set("fixedField", BaseEncoding.base64Url().encode("1234567890123456".getBytes))

  "ToTableRow" should "convert a SpecificRecord to TableRow" in {
    val specificRecord = AvroExample
      .newBuilder()
      .setBooleanField(true)
      .setStringField("someString")
      .setDoubleField(1.0)
      .setLongField(1L)
      .setIntField(1)
      .setFloatField(1f)
      .setBytesField(ByteBuffer.wrap(ByteString.copyFromUtf8("someBytes").toByteArray))
      .setArrayField(List(NestedAvro.newBuilder().setNestedField("nestedValue").build()).asJava)
      .setUnionField("someUnion")
      .setMapField(
        Map("mapKey" -> 1.0d).asJava
          .asInstanceOf[java.util.Map[java.lang.CharSequence, java.lang.Double]]
      )
      .setEnumField(Kind.FOO)
      .setFixedField(new fixedType("1234567890123456".getBytes()))
      .build()

    AvroConverters.toTableRow(specificRecord) shouldEqual expectedOutput
  }

  it should "convert a GenericRecord to TableRow" in {
    val nestedAvro = new GenericData.Record(NestedAvro.SCHEMA$)
    nestedAvro.put("nestedField", "nestedValue")

    val genericRecord = new GenericData.Record(AvroExample.SCHEMA$)
    genericRecord.put("booleanField", true)
    genericRecord.put("stringField", "someString")
    genericRecord.put("doubleField", 1.0)
    genericRecord.put("longField", 1L)
    genericRecord.put("intField", 1)
    genericRecord.put("floatField", 1f)
    genericRecord.put(
      "bytesField",
      ByteBuffer.wrap(ByteString.copyFromUtf8("someBytes").toByteArray)
    )
    genericRecord.put("arrayField", List(nestedAvro).asJava)
    genericRecord.put("unionField", "someUnion")
    genericRecord.put(
      "mapField",
      Map("mapKey" -> 1.0d).asJava
        .asInstanceOf[java.util.Map[java.lang.CharSequence, java.lang.Double]]
    )
    genericRecord.put("enumField", Kind.FOO)
    genericRecord.put("fixedField", new fixedType("1234567890123456".getBytes()))

    AvroConverters.toTableRow(genericRecord) shouldEqual expectedOutput
  }

  val date = LocalDate.parse("2019-10-29")
  val timeMillis: LocalTime = LocalTime.parse("01:24:52.211")
  val timeMicros = 1234L
  val timestampMillis: DateTime = DateTime.parse("2019-10-29T05:24:52.215")
  val timestampMicros = 4325L
  val decimal = new JBigDecimal("3.14")

  val expectedLogicalTypeOutput = new TableRow()
    .set("intField", 1)
    .set("stringField", "someString")
    .set("booleanField", true)
    .set("longField", 1L)
    .set("doubleField", 1.0)
    .set("floatField", 1f)
    .set("bytesField", BaseEncoding.base64Url().encode("someBytes".getBytes))
    .set("dateField", "2019-10-29")
    .set("decimalField", decimal.toString)
    .set("timeMillisField", "01:24:52.211000")
    .set("timeMicrosField", timeMicros)
    .set("timestampMillisField", "2019-10-29T05:24:52.215000")
    .set("timestampMicrosField", timestampMicros)

  "ToTableRowWithLogicalType" should "convert a SpecificRecord with Logical Types to TableRow" in {
    val specificRecord = AvroExampleWithLogicalType
      .newBuilder()
      .setBooleanField(true)
      .setStringField("someString")
      .setDoubleField(1.0)
      .setLongField(1L)
      .setIntField(1)
      .setFloatField(1f)
      .setBytesField(ByteBuffer.wrap(ByteString.copyFromUtf8("someBytes").toByteArray))
      .setDateField(date)
      .setTimeMillisField(timeMillis)
      .setTimeMicrosField(timeMicros)
      .setTimestampMillisField(timestampMillis)
      .setTimestampMicrosField(timestampMicros)
      .setDecimalField(decimal)
      .build()

    AvroConverters.toTableRow(specificRecord) shouldEqual expectedLogicalTypeOutput
  }
}
