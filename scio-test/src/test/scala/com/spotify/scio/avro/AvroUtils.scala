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

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}

import scala.jdk.CollectionConverters._

object AvroUtils {

  val schema: Schema = TestRecord.getClassSchema

  def newGenericRecord(i: Int): GenericRecord = new GenericRecordBuilder(schema)
    .set("int_field", i)
    .set("long_field", i.toLong)
    .set("float_field", i.toFloat)
    .set("double_field", i.toDouble)
    .set("boolean_field", true)
    .set("string_field", "hello")
    .set("array_field", List[CharSequence]("a", "b", "c").asJava)
    .build()

  def newSpecificRecord(i: Int): TestRecord = new TestRecord(
    i,
    i.toLong,
    i.toFloat,
    i.toDouble,
    true,
    "hello",
    List[CharSequence]("a", "b", "c").asJava
  )
}
