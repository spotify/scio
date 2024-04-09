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

package com.spotify.scio.avro

import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.avro.{Schema, SchemaBuilder}

import scala.jdk.CollectionConverters._

object AvroUtils {

  val schema: Schema = SchemaBuilder
    .record("GenericTestRecord")
    .fields()
    .optionalInt("int_field")
    .optionalLong("long_field")
    .optionalFloat("float_field")
    .optionalDouble("double_field")
    .optionalBoolean("boolean_field")
    .optionalString("string_field")
    .name("array_field")
    .`type`
    .array()
    .items()
    .stringType()
    .noDefault()
    .endRecord()

  def newGenericRecord(i: Int): GenericRecord =
    new GenericRecordBuilder(schema)
      .set("int_field", 1 * i)
      .set("long_field", 1L * i)
      .set("float_field", 1f * i)
      .set("double_field", 1.0 * i)
      .set("boolean_field", true)
      .set("string_field", "hello")
      .set("array_field", List[CharSequence]("a", "b", "c").asJava)
      .build()

  def newSpecificRecord(i: Int): TestRecord =
    new TestRecord(
      i,
      i.toLong,
      i.toFloat,
      i.toDouble,
      true,
      "hello",
      List[CharSequence]("a", "b", "c").asJava
    )
}
