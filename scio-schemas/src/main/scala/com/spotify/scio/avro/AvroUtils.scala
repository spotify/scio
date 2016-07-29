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

package com.spotify.scio.avro

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.JavaConverters._

object AvroUtils {

  private def f(name: String, tpe: Schema.Type) =
    new Schema.Field(
      name,
      Schema.createUnion(List(Schema.create(Schema.Type.NULL), Schema.create(tpe)).asJava),
      null, null)

  val schema = Schema.createRecord("GenericTestRecord", null, null, false)
  schema.setFields(List(
    f("int_field", Schema.Type.INT),
    f("long_field", Schema.Type.LONG),
    f("float_field", Schema.Type.FLOAT),
    f("double_field", Schema.Type.DOUBLE),
    f("boolean_field", Schema.Type.BOOLEAN),
    f("string_field", Schema.Type.STRING)
  ).asJava)

  def newGenericRecord(i: Int): GenericRecord = {

    val r = new GenericData.Record(schema)
    r.put("int_field", 1 * i)
    r.put("long_field", 1L * i)
    r.put("float_field", 1F * i)
    r.put("double_field", 1.0 * i)
    r.put("boolean_field", true)
    r.put("string_field", "hello")
    r
  }

  def newSpecificRecord(i: Int): TestRecord =
    new TestRecord(i, i.toLong, i.toFloat, i.toDouble, true, "hello")

}
