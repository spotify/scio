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

package com.spotify.scio.bigquery.types

import com.spotify.scio.bigquery.BigQueryUtil.parseSchema
import org.scalatest.{Matchers, FlatSpec}

class SchemaProviderTest extends FlatSpec with Matchers {

  import Schemas._

  private def basicFields(mode: String) =
    s"""
       |"fields": [
       |  {"mode": "$mode", "name": "boolF", "type": "BOOLEAN"},
       |  {"mode": "$mode", "name": "intF", "type": "INTEGER"},
       |  {"mode": "$mode", "name": "longF", "type": "INTEGER"},
       |  {"mode": "$mode", "name": "floatF", "type": "FLOAT"},
       |  {"mode": "$mode", "name": "doubleF", "type": "FLOAT"},
       |  {"mode": "$mode", "name": "stringF", "type": "STRING"},
       |  {"mode": "$mode", "name": "byteArrayF", "type": "BYTES"},
       |  {"mode": "$mode", "name": "byteStringF", "type": "BYTES"},
       |  {"mode": "$mode", "name": "timestampF", "type": "TIMESTAMP"},
       |  {"mode": "$mode", "name": "dateF", "type": "DATE"},
       |  {"mode": "$mode", "name": "timeF", "type": "TIME"},
       |  {"mode": "$mode", "name": "datetimeF", "type": "DATETIME"}
       |]
       |""".stripMargin

  "SchemaProvider.toSchema" should "support required primitive types" in {
    SchemaProvider.schemaOf[Required] should equal (parseSchema(s"{${basicFields("REQUIRED")}}"))
  }

  it should "support nullable primitive types" in {
    SchemaProvider.schemaOf[Optional] should equal (parseSchema(s"{${basicFields("NULLABLE")}}"))
  }

  it should "support repeated primitive types" in {
    SchemaProvider.schemaOf[Repeated] should equal (parseSchema(s"{${basicFields("REPEATED")}}"))
  }

  private def recordFields(mode: String) =
    s"""
       |{
       |  "fields": [
       |    {"mode": "$mode", "name": "required", "type": "RECORD", ${basicFields("REQUIRED")}},
       |    {"mode": "$mode", "name": "optional", "type": "RECORD", ${basicFields("NULLABLE")}},
       |    {"mode": "$mode", "name": "repeated", "type": "RECORD", ${basicFields("REPEATED")}}
       |  ]
       |}
       |""".stripMargin

  it should "support required records" in {
    SchemaProvider.schemaOf[RequiredNested] should equal (parseSchema(recordFields("REQUIRED")))
  }

  it should "support nullable records" in {
    SchemaProvider.schemaOf[OptionalNested] should equal (parseSchema(recordFields("NULLABLE")))
  }

  it should "support repeated records" in {
    SchemaProvider.schemaOf[RepeatedNested] should equal (parseSchema(recordFields("REPEATED")))
  }

}
