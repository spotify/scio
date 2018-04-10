/*
 * Copyright 2018 Spotify AB.
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

// scalastyle:off line.size.limit
package com.spotify.scio.bigquery.types

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class SchemaUtilTest extends FlatSpec with Matchers {

  def newSchema(mode: String): TableSchema =
    new TableSchema().setFields(List(
      new TableFieldSchema().setName("boolF").setType("BOOLEAN").setMode(mode),
      new TableFieldSchema().setName("intF").setType("INTEGER").setMode(mode),
      new TableFieldSchema().setName("floatF").setType("FLOAT").setMode(mode),
      new TableFieldSchema().setName("stringF").setType("STRING").setMode(mode),
      new TableFieldSchema().setName("bytesF").setType("BYTES").setMode(mode),
      new TableFieldSchema().setName("timestampF").setType("TIMESTAMP").setMode(mode),
      new TableFieldSchema().setName("dateF").setType("DATE").setMode(mode),
      new TableFieldSchema().setName("timeF").setType("TIME").setMode(mode),
      new TableFieldSchema().setName("datetimeF").setType("DATETIME").setMode(mode)
    ).asJava)

  "toPrettyString()" should "support required primitive types" in {
    SchemaUtil.toPrettyString(newSchema("REQUIRED"), "Row", 0) should equal (
      """
        |@BigQueryType.toTable
        |case class Row(boolF: Boolean, intF: Long, floatF: Double, stringF: String, bytesF: ByteString, timestampF: Instant, dateF: LocalDate, timeF: LocalTime, datetimeF: LocalDateTime)
      """.stripMargin.trim)
  }

  it should "support nullable primitive types" in {
    // scalastyle:off line.size.limit
    SchemaUtil.toPrettyString(newSchema("NULLABLE"), "Row", 0) should equal (
      """
        |@BigQueryType.toTable
        |case class Row(boolF: Option[Boolean], intF: Option[Long], floatF: Option[Double], stringF: Option[String], bytesF: Option[ByteString], timestampF: Option[Instant], dateF: Option[LocalDate], timeF: Option[LocalTime], datetimeF: Option[LocalDateTime])
      """.stripMargin.trim)
    // scalastyle:on line.size.limit
  }

  it should "support repeated primitive types" in {
    // scalastyle:off line.size.limit
    SchemaUtil.toPrettyString(newSchema("REPEATED"), "Row", 0) should equal (
      """
        |@BigQueryType.toTable
        |case class Row(boolF: List[Boolean], intF: List[Long], floatF: List[Double], stringF: List[String], bytesF: List[ByteString], timestampF: List[Instant], dateF: List[LocalDate], timeF: List[LocalTime], datetimeF: List[LocalDateTime])
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
    SchemaUtil.toPrettyString(newSchema("REQUIRED"), "Row", 2) should equal (
      """
        |@BigQueryType.toTable
        |case class Row(
        |  boolF: Boolean,
        |  intF: Long,
        |  floatF: Double,
        |  stringF: String,
        |  bytesF: ByteString,
        |  timestampF: Instant,
        |  dateF: LocalDate,
        |  timeF: LocalTime,
        |  datetimeF: LocalDateTime)
      """.stripMargin.trim)
  }

  it should "support reserved words" in {
    val expectedFields = SchemaUtil.scalaReservedWords
      .map(e => s"`$e`").mkString(start = "", sep = ": Long, ", end = ": Long")
    val schema = new TableSchema().setFields(
      SchemaUtil.scalaReservedWords.map { name =>
        new TableFieldSchema().setName(name).setType("INTEGER").setMode("REQUIRED")
      }.asJava)
    SchemaUtil.toPrettyString(schema, "Row", 0) should equal (
      s"""|@BigQueryType.toTable
          |case class Row($expectedFields)""".stripMargin
    )
  }

}
// scalastyle:on line.size.limit
