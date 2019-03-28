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
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ToTableSchemaTest extends FlatSpec with Matchers with ToTableSchema {

  "toTableSchema" should "convert an Avro Schema to a BigQuery TableSchema" in {
    toTableSchema(AvroExample.SCHEMA$) shouldEqual
      new TableSchema().setFields(List(
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
          .setFields(List(new TableFieldSchema()
            .setName("nestedField")
            .setMode("REQUIRED")
            .setType("STRING")).asJava),
        new TableFieldSchema()
          .setName("mapField")
          .setType("RECORD")
          .setMode("REPEATED")
          .setFields(List(
            new TableFieldSchema()
              .setName("key")
              .setType("STRING")
              .setMode("REQUIRED"),
            new TableFieldSchema()
              .setName("value")
              .setType("FLOAT")
              .setMode("REQUIRED")
          ).asJava),
        new TableFieldSchema()
          .setName("enumField")
          .setType("STRING")
          .setMode("REQUIRED"),
        new TableFieldSchema()
          .setName("fixedField")
          .setType("BYTES")
          .setMode("REQUIRED")
      ).asJava)
  }
}
