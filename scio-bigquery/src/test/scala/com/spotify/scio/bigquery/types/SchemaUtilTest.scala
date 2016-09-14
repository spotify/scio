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

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class SchemaUtilTest extends FlatSpec with Matchers {

  def newSchema(mode: String): TableSchema =
    new TableSchema().setFields(List(
      new TableFieldSchema().setName("f1").setType("INTEGER").setMode(mode),
      new TableFieldSchema().setName("f2").setType("FLOAT").setMode(mode),
      new TableFieldSchema().setName("f3").setType("BOOLEAN").setMode(mode),
      new TableFieldSchema().setName("f4").setType("STRING").setMode(mode),
      new TableFieldSchema().setName("f5").setType("TIMESTAMP").setMode(mode)
    ).asJava)

  "toPrettyString()" should "support required primitive types" in {
    SchemaUtil.toPrettyString(newSchema("REQUIRED"), "Row", 0) should equal(
      """
        |@BigQueryType.toTable
        |case class Row(f1: Long, f2: Double, f3: Boolean, f4: String, f5: Instant)
      """.stripMargin.trim)
  }

  it should "support nullable primitive types" in {
    // scalastyle:off line.size.limit
    SchemaUtil.toPrettyString(newSchema("NULLABLE"), "Row", 0) should equal(
      """
        |@BigQueryType.toTable
        |case class Row(f1: Option[Long], f2: Option[Double], f3: Option[Boolean], f4: Option[String], f5: Option[Instant])
      """.stripMargin.trim)
    // scalastyle:on line.size.limit
  }

  it should "support repeated primitive types" in {
    // scalastyle:off line.size.limit
    SchemaUtil.toPrettyString(newSchema("REPEATED"), "Row", 0) should equal(
      """
        |@BigQueryType.toTable
        |case class Row(f1: List[Long], f2: List[Double], f3: List[Boolean], f4: List[String], f5: List[Instant])
      """.stripMargin.trim)
    // scalastyle:on line.size.limit
  }

  it should "support records" in {
    val fields = List(
      new TableFieldSchema().setName("f1").setType("INTEGER").setMode("REQUIRED"),
      new TableFieldSchema().setName("f2").setType("FLOAT").setMode("REQUIRED")
    ).asJava
    val schema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("r1").setType("RECORD").setFields(fields).setMode("REQUIRED"),
      new TableFieldSchema().setName("r2").setType("RECORD").setFields(fields).setMode("NULLABLE"),
      new TableFieldSchema().setName("r3").setType("RECORD").setFields(fields).setMode("REPEATED")
    ).asJava)
    SchemaUtil.toPrettyString(schema, "Row", 0) should equal (
      """
        |@BigQueryType.toTable
        |case class Row(r1: R1$1, r2: Option[R2$1], r3: List[R3$1])
        |case class R1$1(f1: Long, f2: Double)
        |case class R2$1(f1: Long, f2: Double)
        |case class R3$1(f1: Long, f2: Double)
      """.stripMargin.trim)
  }

  it should "support indent" in {
    SchemaUtil.toPrettyString(newSchema("REQUIRED"), "Row", 2) should equal(
      """
        |@BigQueryType.toTable
        |case class Row(
        |  f1: Long,
        |  f2: Double,
        |  f3: Boolean,
        |  f4: String,
        |  f5: Instant)
      """.stripMargin.trim)
  }

}
