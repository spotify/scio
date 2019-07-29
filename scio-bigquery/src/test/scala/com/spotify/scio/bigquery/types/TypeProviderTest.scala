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

package com.spotify.scio.bigquery.types

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.util.SerializableUtils
import org.joda.time.Instant
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._

object TypeProviderTest {
  @BigQueryType.toTable
  case class RefinedClass(a1: Int)

  @BigQueryType.fromSchema("""
      |{"fields": [{"mode": "REQUIRED", "name": "f1", "type": "INTEGER"}]}
      |""".stripMargin)
  class S1

  @BigQueryType.fromSchema("""
       |{"fields": [{"mode": "REQUIRED", "name": "f1", "type": "INTEGER"}]}
       |""".stripMargin)
  class S2

  @BigQueryType.fromSchema("""
      |{"fields": [{"mode": "REQUIRED", "name": "f1", "type": "INTEGER"}]}
      |""".stripMargin)
  class S3

  @BigQueryType.fromSchema("""
      |{"fields": [{"mode": "REQUIRED", "name": "f1", "type": "INTEGER"}]}
      |""".stripMargin)
  @description("Table S4")
  class S4

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "REPEATED", "name": "f1", "type": "INTEGER", "description": "description"}
      |  ]
      |}
    """.stripMargin
  )
  class RecordWithDescription
}

// TODO: mock BigQueryClient for fromTable and fromQuery
class TypeProviderTest extends AnyFlatSpec with Matchers {
  val NOW = Instant.now()

  import TypeProviderTest._

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

  it should "support table description" in {
    S4.tableDescription shouldBe "Table S4"
  }

  it should "be serializable" in {
    SerializableUtils.ensureSerializable(S1(1))
  }

  it should "infer the same schema" in {
    BigQueryType[RecordWithDescription].schema shouldBe RecordWithDescription.schema
  }

  "BigQueryTag" should "be a serializable annotation" in {
    SerializableUtils.ensureSerializable[BigQueryTag](new BigQueryTag())
  }

  @BigQueryType.fromSchema("""
      |{"fields": [{"name": "f1", "type": "INTEGER"}]}
      |""".stripMargin)
  class MissingMode

  it should "support missing mode" in {
    val r = MissingMode(Some(1))
    r.f1 shouldBe Some(1)
  }

  @BigQueryType.fromSchema("""
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
    r1 shouldBe r2
  }

  @BigQueryType.fromSchema("""
      |{
      |  "fields": [ {"mode": "REQUIRED", "name": "f1", "type": "INTEGER"} ]
      |}
    """.stripMargin)
  class Artisanal1Field

  it should "not provide .tupled in companion object with single field" in {
    Artisanal1Field.getClass.getMethods
      .map(_.getName) should not contain "tupled"
  }

  it should "support .schema in companion object" in {
    RecordWithRequiredPrimitives.schema should not be null
  }

  it should "support .fromTableRow in companion object" in {
    (classOf[TableRow => RecordWithRequiredPrimitives]
      isAssignableFrom RecordWithRequiredPrimitives.fromTableRow.getClass) shouldBe true
  }

  it should "support .toTableRow in companion object" in {
    (classOf[ToTable => RecordWithRequiredPrimitives]
      isAssignableFrom RecordWithRequiredPrimitives.toTableRow.getClass) shouldBe true
  }

  @BigQueryType.fromSchema("""
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
    r1.f1 shouldBe Some(1L)
    r1.f2 shouldBe Some(1.5)
    r1.f3 shouldBe Some(true)
    r1.f4 shouldBe Some("hello")
    r1.f5 shouldBe Some(NOW)

    val r2 = RecordWithNullablePrimitives(None, None, None, None, None)
    r2.f1 shouldBe None
    r2.f2 shouldBe None
    r2.f3 shouldBe None
    r2.f4 shouldBe None
    r2.f5 shouldBe None
  }

  @BigQueryType.fromSchema("""
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
      List(1L, 2L),
      List(1.5, 2.5),
      List(true, false),
      List("hello", "world"),
      List(NOW, NOW.plus(1000))
    )
    r1.f1 shouldBe List(1L, 2L)
    r1.f2 shouldBe List(1.5, 2.5)
    r1.f3 shouldBe List(true, false)
    r1.f4 shouldBe List("hello", "world")
    r1.f5 shouldBe List(NOW, NOW.plus(1000))

    val r2 = RecordWithRepeatedPrimitives(Nil, Nil, Nil, Nil, Nil)
    r2.f1 shouldBe Nil
    r2.f2 shouldBe Nil
    r2.f3 shouldBe Nil
    r2.f4 shouldBe Nil
    r2.f5 shouldBe Nil
  }

  @BigQueryType.fromSchema("""
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
    val r =
      RecordWithRequiredRecords(F1$1(1L), F2$1(Some(1L)), F3$1(List(1L, 2L)))
    r.f1.g shouldBe 1L
    r.f2.g shouldBe Some(1L)
    r.f3.g shouldBe List(1L, 2L)
  }

  @BigQueryType.fromSchema("""
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
    val r =
      RecordWithNullableRecords(Some(F1$2(1L)), Some(F2$2(Some(1L))), Some(F3$2(List(1L, 2L))))
    r.f1.get.g shouldBe 1L
    r.f2.get.g shouldBe Some(1L)
    r.f3.get.g shouldBe List(1L, 2L)
  }

  @BigQueryType.fromSchema("""
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
    val r =
      RecordWithRepeatedRecords(List(F1$3(1L)), List(F2$3(Some(1L))), List(F3$3(List(1L, 2L))))
    r.f1 shouldBe List(F1$3(1L))
    r.f2 shouldBe List(F2$3(Some(1L)))
    r.f3 shouldBe List(F3$3(List(1L, 2L)))
  }

  @BigQueryType.toTable
  case class ToTable(f1: Long, f2: Double, f3: Boolean, f4: String, f5: Instant)

  @BigQueryType.toTable
  case class Record(f1: Int, f2: String)

  "BigQueryType.toTable" should "support .tupled in companion object" in {
    val r1 = ToTable(1L, 1.5, true, "hello", NOW)
    val r2 = ToTable.tupled((1L, 1.5, true, "hello", NOW))
    r1 shouldBe r2
  }

  it should "support .schema in companion object" in {
    ToTable.schema should not be null
  }

  it should "support .fromTableRow in companion object" in {
    (classOf[TableRow => ToTable] isAssignableFrom ToTable.fromTableRow.getClass) shouldBe true
  }

  it should "support .toTableRow in companion object" in {
    (classOf[ToTable => TableRow] isAssignableFrom ToTable.toTableRow.getClass) shouldBe true
  }

  it should "create companion object that is a Function subtype" in {
    val cls5 =
      classOf[Function5[Long, Double, Boolean, String, Instant, ToTable]]
    val cls2 = classOf[Function2[Int, String, Record]]
    (cls5 isAssignableFrom ToTable.getClass) shouldBe true
    (cls2 isAssignableFrom Record.getClass) shouldBe true
  }

  it should "create companion object that is functionally equal to its apply method" in {
    def doApply(f: (Int, String) => Record)(x: (Int, String)): Record =
      f(x._1, x._2)

    doApply(Record.apply _)((3, "a")) shouldBe doApply(Record)((3, "a"))
    doApply(Record)((3, "a")) shouldBe Record(3, "a")
  }

  // This should compile
  @BigQueryType.toTable
  case class RecordWithDefault(x: Int, y: Int = 2)

  it should "work for case class with default params" in {
    classOf[Function2[Int, Int, RecordWithDefault]] isAssignableFrom RecordWithDefault.getClass
  }

  it should "support default argument correctly" in {
    RecordWithDefault(10).y shouldBe 2
  }

  @BigQueryType.fromSchema("""
      |{
      |  "fields": [
      |    {"mode": "REQUIRED", "name": "f1", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f2", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f3", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f4", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f5", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f6", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f7", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f8", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f9", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f10", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f11", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f12", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f13", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f14", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f15", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f16", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f17", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f18", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f19", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f20", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f21", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f22", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f23", "type": "INTEGER"}
      |  ]
      |}
    """.stripMargin)
  class ArtisanalMoreThan22Fields

  "BigQueryType.fromSchema" should "support .schema in companion object with >22 fields" in {
    ArtisanalMoreThan22Fields(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23)
    ArtisanalMoreThan22Fields.schema should not be null
  }

  it should "not provide .tupled in companion object with >22 fields" in {
    ArtisanalMoreThan22Fields.getClass.getMethods
      .map(_.getName) should not contain "tupled"
  }

  @BigQueryType.toTable
  case class TwentyThree(
    a1: Int,
    a2: Int,
    a3: Int,
    a4: Int,
    a5: Int,
    a6: Int,
    a7: Int,
    a8: Int,
    a9: Int,
    a10: Int,
    a11: Int,
    a12: Int,
    a13: Int,
    a14: Int,
    a15: Int,
    a16: Int,
    a17: Int,
    a18: Int,
    a19: Int,
    a20: Int,
    a21: Int,
    a22: Int,
    a23: Int
  )

  "BigQueryType.toTable" should "not provide .tupled in companion object with >22 fields" in {
    TwentyThree.getClass.getMethods.map(_.getName) should not contain "tupled"
  }

  it should "support .schema in companion object with >22 fields" in {
    TwentyThree.schema should not be null
  }

  it should "support .fromTableRow in companion object with >22 fields" in {
    val cls = classOf[TableRow => TwentyThree]
    (cls isAssignableFrom TwentyThree.fromTableRow.getClass) shouldBe true
  }

  it should "support .toTableRow in companion object with >22 fields" in {
    val cls = classOf[TwentyThree => TableRow]
    (cls isAssignableFrom TwentyThree.toTableRow.getClass) shouldBe true
  }

  @BigQueryType.toTable
  @description("Foo bar table description")
  case class DescriptionTbl(a1: Int)

  it should "support table description" in {
    DescriptionTbl.tableDescription shouldBe "Foo bar table description"
  }

  @BigQueryType.fromSchema("""
      |{
      |  "fields": [ {"mode": "REQUIRED", "name": "f1", "type": "INTEGER"} ]
      |}
    """.stripMargin)
  class Artisanal1FieldWithBody {
    object InnerObject {
      def innerObjectInnerMethod: String = "so artisanal"
    }
    val bar: Long = 42L
    def foo: String = "foo"
  }

  it should "support user defined body in fromSchema" in {
    Artisanal1Field.getClass.getMethods
      .map(_.getName) should not contain "tupled"
    RecordWithRequiredPrimitives.schema should not be null
    (classOf[TableRow => RecordWithRequiredPrimitives]
      isAssignableFrom RecordWithRequiredPrimitives.fromTableRow.getClass) shouldBe true
    (classOf[ToTable => RecordWithRequiredPrimitives]
      isAssignableFrom RecordWithRequiredPrimitives.toTableRow.getClass) shouldBe true
    Artisanal1FieldWithBody(3).bar shouldBe 42L
    Artisanal1FieldWithBody(3).foo shouldBe "foo"
    Artisanal1FieldWithBody(3).InnerObject.innerObjectInnerMethod shouldBe "so artisanal"
  }

  @BigQueryType.toTable
  case class Artisanal1ToTableWithBody(a1: Int) {
    object InnerObject {
      def innerObjectInnerMethod: String = "so artisanal"
    }
    val bar: Long = 42L
    def foo: String = "foo"
  }

  object Artisanal1ToTableWithBody {
    def foo(o: Artisanal1ToTableWithBody): Int = o.a1
  }

  it should "support user defined body in toTable and custom object method" in {
    classOf[Function1[Int, Artisanal1ToTableWithBody]] isAssignableFrom
      Artisanal1ToTableWithBody.getClass
    Artisanal1ToTableWithBody(10).a1 shouldBe 10
    Artisanal1ToTableWithBody(3).bar shouldBe 42L
    Artisanal1ToTableWithBody(3).foo shouldBe "foo"
    Artisanal1ToTableWithBody(3).InnerObject.innerObjectInnerMethod shouldBe "so artisanal"
    Artisanal1ToTableWithBody.foo(Artisanal1ToTableWithBody(3)) shouldBe 3
  }

  class Annotation1 extends StaticAnnotation
  class Annotation2 extends StaticAnnotation

  def containsAllAnnotTypes[T: TypeTag]: Assertion =
    typeOf[T].typeSymbol.annotations
      .map(_.tree.tpe)
      .containsSlice(Seq(typeOf[Annotation1], typeOf[Annotation2])) shouldBe true

  @Annotation1
  @BigQueryType.toTable
  @Annotation2
  case class RecordWithSurroundingAnnotations(a1: Int)

  it should "preserve surrounding user defined annotations" in {
    containsAllAnnotTypes[RecordWithSurroundingAnnotations]
  }

  @BigQueryType.toTable
  @Annotation1
  @Annotation2
  case class RecordWithSequentialAnnotations(a1: Int)

  it should "preserve sequential user defined annotations" in {
    containsAllAnnotTypes[RecordWithSequentialAnnotations]
  }

  @Annotation1
  @BigQueryType.fromSchema("""
      |{"fields": [ {"mode": "REQUIRED", "name": "f1", "type": "DATE"} ]}
      |""".stripMargin)
  @Annotation2
  class SchemaWithSurroundingAnnotations

  "BigQueryType.fromSchema" should "preserve surrounding user defined annotations" in {
    containsAllAnnotTypes[SchemaWithSurroundingAnnotations]
  }

  @BigQueryType.fromSchema("""
      |{"fields": [ {"mode": "REQUIRED", "name": "f1", "type": "DATE"} ]}
      |""".stripMargin)
  @Annotation1
  @Annotation2
  class SchemaWithSequentialAnnotations

  it should "preserve sequential user defined annotations" in {
    containsAllAnnotTypes[SchemaWithSequentialAnnotations]
  }

  "BigQueryType" should " #1414: not fail on refined types" in {
    noException should be thrownBy
      BigQueryType[TypeProviderTest.RefinedClass with BigQueryType.HasAnnotation]
  }


  @BigQueryType.fromSchema(
    """
      |{"fields": [{"mode": "REQUIRED", "name": "f1", "type": "GEOGRAPHY"}]}
    """.stripMargin)
  class GeoRecordFrom

  @BigQueryType.toTable
  case class GeoRecordTo(f1: Geography)

  it should "#1882: support GEOGRAPHY type" in {
    val wkt = "POINT (30 10)"
    val cc = GeoRecordFrom(Geography(wkt))
    cc.f1 shouldBe Geography(wkt)
    GeoRecordTo(Geography(wkt))
  }
}
