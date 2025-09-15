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

package com.spotify.scio.bigquery

import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class BigQueryUtilTest extends AnyFlatSpec with Matchers {
  "parseSchema" should "work" in {
    val schema = new TableSchema().setFields(
      List(
        new TableFieldSchema()
          .setName("f1")
          .setType("INTEGER")
          .setMode("REQUIRED"),
        new TableFieldSchema()
          .setName("f2")
          .setType("FLOAT")
          .setMode("NULLABLE"),
        new TableFieldSchema()
          .setName("f3")
          .setType("TIMESTAMP")
          .setMode("REPEATED"),
        new TableFieldSchema()
          .setName("f4")
          .setMode("RECORD")
          .setFields(
            List(
              new TableFieldSchema()
                .setName("f5")
                .setType("BOOLEAN")
                .setMode("REQUIRED"),
              new TableFieldSchema()
                .setName("f6")
                .setType("STRING")
                .setMode("NULLABLE"),
              new TableFieldSchema()
                .setName("f6")
                .setType("STRING")
                .setMode("REPEATED")
            ).asJava
          )
      ).asJava
    )
    schema.setFactory(GsonFactory.getDefaultInstance)
    BigQueryUtil.parseSchema(schema.toString) shouldBe schema
  }
}
