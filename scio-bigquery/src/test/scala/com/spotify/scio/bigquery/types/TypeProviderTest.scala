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

import com.google.api.services.bigquery.model.TableRow
import org.joda.time.Instant
import org.scalatest.{FlatSpec, Matchers}

// TODO: figure out how to make IntelliJ happy
// TODO: mock BigQueryClient for fromTable and fromQuery
class TypeProviderTest extends FlatSpec with Matchers {

  val NOW = Instant.now()

  @BigQueryType.fromSchema(
    """{"fields": [{"mode": "REQUIRED", "name": "f1", "type": "INTEGER"}]}""")
  class S1

  @BigQueryType.fromSchema(
    """
       {"fields": [{"mode": "REQUIRED", "name": "f1", "type": "INTEGER"}]}
    """)
  class S2

  @BigQueryType.fromSchema(
    """
      |{"fields": [{"mode": "REQUIRED", "name": "f1", "type": "INTEGER"}]}
    """.stripMargin)
  class S3

  "BigQueryType.fromSchema" should "support string literal" in {
    val r = S1(1L)
    r.f1 shouldBe 1L
  }

  it should "support multi-line string literal" in {
    val r = S2(1L)
    r.f1 shouldBe 1L
  }

  it should "support multi-line string literal with stripMargin" in {
    val r = S3(1L)
    r.f1 shouldBe 1L
  }

  @BigQueryType.fromSchema("""{"fields": [{"name": "f1", "type": "INTEGER"}]}""")
  class MissingMode

  it should "support missing mode" in {
    val r = MissingMode(Some(1))
    r.f1 should equal (Some(1))
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "REQUIRED", "name": "f1", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f2", "type": "FLOAT"},
      |    {"mode": "REQUIRED", "name": "f3", "type": "BOOLEAN"},
      |    {"mode": "REQUIRED", "name": "f4", "type": "STRING"},
      |    {"mode": "REQUIRED", "name": "f5", "type": "TIMESTAMP"}
      |    ]
      |}
    """.stripMargin)
  class RecordWithRequiredPrimitives

  it should "support required primitive types" in {
    val r = RecordWithRequiredPrimitives(1L, 1.5, true, "hello", NOW)
    r.f1 shouldBe 1L
    r.f2 shouldBe 1.5
    r.f3 shouldBe true
    r.f4 shouldBe "hello"
    r.f5 shouldBe NOW
  }

  it should "support .tupled in companion object" in {
    val r1 = RecordWithRequiredPrimitives(1L, 1.5, true, "hello", NOW)
    val r2 = RecordWithRequiredPrimitives.tupled((1L, 1.5, true, "hello", NOW))
    r1 should equal (r2)
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [ {"mode": "REQUIRED", "name": "f1", "type": "INTEGER"} ]
      |}
    """.stripMargin)
  class Artisanal1Field

  it should "not provide .tupled in companion object with single field" in {
    Artisanal1Field.getClass.getMethods.map(_.getName) should not contain "tupled"
  }

  it should "support .schema in companion object" in {
    RecordWithRequiredPrimitives.schema should not be null
  }

  it should "support .fromTableRow in companion object" in {
    (classOf[(TableRow => RecordWithRequiredPrimitives)]
      isAssignableFrom RecordWithRequiredPrimitives.fromTableRow.getClass) shouldBe true
  }

  it should "support .toTableRow in companion object" in {
    (classOf[(ToTable => RecordWithRequiredPrimitives)]
      isAssignableFrom RecordWithRequiredPrimitives.toTableRow.getClass) shouldBe true
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "NULLABLE", "name": "f1", "type": "INTEGER"},
      |    {"mode": "NULLABLE", "name": "f2", "type": "FLOAT"},
      |    {"mode": "NULLABLE", "name": "f3", "type": "BOOLEAN"},
      |    {"mode": "NULLABLE", "name": "f4", "type": "STRING"},
      |    {"mode": "NULLABLE", "name": "f5", "type": "TIMESTAMP"}
      |  ]
      |}
    """.stripMargin)
  class RecordWithNullablePrimitives

  it should "support nullable primitive types" in {
    val r1 = RecordWithNullablePrimitives(Some(1L), Some(1.5), Some(true), Some("hello"), Some(NOW))
    r1.f1 should equal (Some(1L))
    r1.f2 should equal (Some(1.5))
    r1.f3 should equal (Some(true))
    r1.f4 should equal (Some("hello"))
    r1.f5 should equal (Some(NOW))

    val r2 = RecordWithNullablePrimitives(None, None, None, None, None)
    r2.f1 shouldBe None
    r2.f2 shouldBe None
    r2.f3 shouldBe None
    r2.f4 shouldBe None
    r2.f5 shouldBe None
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "REPEATED", "name": "f1", "type": "INTEGER"},
      |    {"mode": "REPEATED", "name": "f2", "type": "FLOAT"},
      |    {"mode": "REPEATED", "name": "f3", "type": "BOOLEAN"},
      |    {"mode": "REPEATED", "name": "f4", "type": "STRING"},
      |    {"mode": "REPEATED", "name": "f5", "type": "TIMESTAMP"}
      |  ]
      |}
    """.stripMargin)
  class RecordWithRepeatedPrimitives

  it should "support repeated primitive types" in {
    val r1 = RecordWithRepeatedPrimitives(
      List(1L, 2L), List(1.5, 2.5), List(true, false), List("hello", "world"),
      List(NOW, NOW.plus(1000)))
    r1.f1 should equal (List(1L, 2L))
    r1.f2 should equal (List(1.5, 2.5))
    r1.f3 should equal (List(true, false))
    r1.f4 should equal (List("hello", "world"))
    r1.f5 should equal (List(NOW, NOW.plus(1000)))

    val r2 = RecordWithRepeatedPrimitives(Nil, Nil, Nil, Nil, Nil)
    r2.f1 shouldBe Nil
    r2.f2 shouldBe Nil
    r2.f3 shouldBe Nil
    r2.f4 shouldBe Nil
    r2.f5 shouldBe Nil
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {
      |      "mode": "REQUIRED", "name": "f1", "type": "RECORD",
      |      "fields": [{"mode": "REQUIRED", "name": "g", "type": "INTEGER"}]
      |    },
      |    {
      |      "mode": "REQUIRED", "name": "f2", "type": "RECORD",
      |      "fields": [{"mode": "NULLABLE", "name": "g", "type": "INTEGER"}]
      |    },
      |    {
      |      "mode": "REQUIRED", "name": "f3", "type": "RECORD",
      |      "fields": [{"mode": "REPEATED", "name": "g", "type": "INTEGER"}]
      |    }
      |  ]
      |}
    """.stripMargin)
  class RecordWithRequiredRecords

  it should "support required records" in {
    val r = RecordWithRequiredRecords(F1$1(1L), F2$1(Some(1L)), F3$1(List(1L, 2L)))
    r.f1.g should equal (1L)
    r.f2.g should equal (Some(1L))
    r.f3.g should equal (List(1L, 2L))
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {
      |      "mode": "NULLABLE", "name": "f1", "type": "RECORD",
      |      "fields": [{"mode": "REQUIRED", "name": "g", "type": "INTEGER"}]
      |    },
      |    {
      |      "mode": "NULLABLE", "name": "f2", "type": "RECORD",
      |      "fields": [{"mode": "NULLABLE", "name": "g", "type": "INTEGER"}]
      |    },
      |    {
      |      "mode": "NULLABLE", "name": "f3", "type": "RECORD",
      |      "fields": [{"mode": "REPEATED", "name": "g", "type": "INTEGER"}]
      |    }
      |  ]
      |}
    """.stripMargin)
  class RecordWithNullableRecords

  it should "support nullable records" in {
    val r = RecordWithNullableRecords(
      Some(F1$2(1L)), Some(F2$2(Some(1L))), Some(F3$2(List(1L, 2L))))
    r.f1.get.g should equal (1L)
    r.f2.get.g should equal (Some(1L))
    r.f3.get.g should equal (List(1L, 2L))
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {
      |      "mode": "REPEATED", "name": "f1", "type": "RECORD",
      |      "fields": [{"mode": "REQUIRED", "name": "g", "type": "INTEGER"}]
      |    },
      |    {
      |      "mode": "REPEATED", "name": "f2", "type": "RECORD",
      |      "fields": [{"mode": "NULLABLE", "name": "g", "type": "INTEGER"}]
      |    },
      |    {
      |      "mode": "REPEATED", "name": "f3", "type": "RECORD",
      |      "fields": [{"mode": "REPEATED", "name": "g", "type": "INTEGER"}]
      |    }
      |  ]
      |}
    """.stripMargin)
  class RecordWithRepeatedRecords

  it should "support repeated records" in {
    val r = RecordWithRepeatedRecords(
      List(F1$3(1L)), List(F2$3(Some(1L))), List(F3$3(List(1L, 2L))))
    r.f1 should equal (List(F1$3(1L)))
    r.f2 should equal (List(F2$3(Some(1L))))
    r.f3 should equal (List(F3$3(List(1L, 2L))))
  }

  @BigQueryType.toTable
  case class ToTable(f1: Long, f2: Double, f3: Boolean, f4: String, f5: Instant)

  @BigQueryType.toTable
  case class Record(f1: Int, f2: String)

  "BigQueryType.toTable" should "support .tupled in companion object" in {
    val r1 = ToTable(1L, 1.5, true, "hello", NOW)
    val r2 = ToTable.tupled((1L, 1.5, true, "hello", NOW))
    r1 should equal (r2)
  }

  it should "support .schema in companion object" in {
    ToTable.schema should not be null
  }

  it should "support .fromTableRow in companion object" in {
    (classOf[(TableRow => ToTable)] isAssignableFrom ToTable.fromTableRow.getClass) shouldBe true
  }

  it should "support .toTableRow in companion object" in {
    (classOf[(ToTable => TableRow)] isAssignableFrom ToTable.toTableRow.getClass) shouldBe true
  }

  it should "create companion object that is a Function subtype" in {
    (classOf[Function5[Long, Double, Boolean, String, Instant, ToTable]] isAssignableFrom ToTable.getClass) shouldBe true
    (classOf[Function2[Int, String, Record]] isAssignableFrom Record.getClass) shouldBe true
  }

  it should "create companion object that is functionally equal to its apply method" in {
    def doApply(f: (Int, String) => Record)(x: (Int, String)): Record = f(x._1, x._2)

    doApply(Record.apply _)((3, "a")) shouldEqual doApply(Record)((3, "a"))
    doApply(Record)((3, "a")) shouldEqual Record(3, "a")
  }

}
