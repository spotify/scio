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

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ToTableSchemaTest extends AnyFlatSpec with Matchers {
  "toTableSchema" should "convert an Avro Schema to a BigQuery TableSchema" in {
    AvroConverters.toTableSchema(AvroExample.SCHEMA$) shouldEqual
      new TableSchema().setFields(
        List(
          new TableFieldSchema()
            .setName("intField")
            .setType("INTEGER")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("stringField")
            .setType("STRING")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("booleanField")
            .setType("BOOLEAN")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("longField")
            .setType("INTEGER")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("doubleField")
            .setType("FLOAT")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("floatField")
            .setType("FLOAT")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("bytesField")
            .setType("BYTES")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("unionField")
            .setType("STRING")
            .setMode("NULLABLE"),
          new TableFieldSchema()
            .setName("arrayField")
            .setType("RECORD")
            .setMode("REPEATED")
            .setDescription("some array doc")
            .setFields(
              List(
                new TableFieldSchema()
                  .setName("nestedField")
                  .setMode("REQUIRED")
                  .setType("STRING")
              ).asJava
            ),
          new TableFieldSchema()
            .setName("mapField")
            .setType("RECORD")
            .setMode("REPEATED")
            .setFields(
              List(
                new TableFieldSchema()
                  .setName("key")
                  .setType("STRING")
                  .setMode("REQUIRED"),
                new TableFieldSchema()
                  .setName("value")
                  .setType("FLOAT")
                  .setMode("REQUIRED")
              ).asJava
            ),
          new TableFieldSchema()
            .setName("enumField")
            .setType("STRING")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("fixedField")
            .setType("BYTES")
            .setMode("REQUIRED")
        ).asJava
      )
  }

  "toTableSchema" should "convert an Avro Schema with Logical Types to a BigQuery TableSchema" in {
    AvroConverters.toTableSchema(AvroExampleWithLogicalType.SCHEMA$) shouldEqual
      new TableSchema().setFields(
        List(
          new TableFieldSchema()
            .setName("intField")
            .setType("INTEGER")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("stringField")
            .setType("STRING")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("booleanField")
            .setType("BOOLEAN")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("longField")
            .setType("INTEGER")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("doubleField")
            .setType("FLOAT")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("floatField")
            .setType("FLOAT")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("bytesField")
            .setType("BYTES")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("decimalField")
            .setType("NUMERIC")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("dateField")
            .setType("DATE")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("timeMillisField")
            .setType("TIME")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("timeMicrosField")
            .setType("INTEGER")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("timestampMillisField")
            .setType("TIMESTAMP")
            .setMode("REQUIRED"),
          new TableFieldSchema()
            .setName("timestampMicrosField")
            .setType("INTEGER")
            .setMode("REQUIRED")
        ).asJava
      )
  }
}
