package com.spotify.cloud.bigquery.types

import com.spotify.cloud.bigquery.TableRow
import org.joda.time.Instant
import org.scalatest.{Matchers, FlatSpec}

// TODO: figure out how to make IntelliJ happy
// TODO: mock BigQueryClient for fromTable and fromQuery
class TypeProviderTest extends FlatSpec with Matchers {

  val NOW = Instant.now()

  @BigQueryType.fromSchema("""{"fields": [{"mode": "REQUIRED", "name": "f1", "type": "INTEGER"}]}""")
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

  "BigQueryEntity.fromSchema" should "support string literal" in {
    val r = S1(1L)
    r.f1 should equal (1L)
  }

  it should "support multi-line string literal" in {
    val r = S2(1L)
    r.f1 should equal (1L)
  }

  it should "support multi-line string literal with stripMargin" in {
    val r = S3(1L)
    r.f1 should equal (1L)
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
    r.f1 should equal (1L)
    r.f2 should equal (1.5)
    r.f3 should equal (true)
    r.f4 should equal ("hello")
    r.f5 should equal (NOW)
  }

  it should "support .tupled in companion" in {
    val r1 = RecordWithRequiredPrimitives(1L, 1.5, true, "hello", NOW)
    val r2 = RecordWithRequiredPrimitives.tupled((1L, 1.5, true, "hello", NOW))
    r1 should equal (r2)
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
    r2.f1 should equal (None)
    r2.f2 should equal (None)
    r2.f3 should equal (None)
    r2.f4 should equal (None)
    r2.f5 should equal (None)
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
      List(1L, 2L), List(1.5, 2.5), List(true, false), List("hello", "world"), List(NOW, NOW.plus(1000)))
    r1.f1 should equal (List(1L, 2L))
    r1.f2 should equal (List(1.5, 2.5))
    r1.f3 should equal (List(true, false))
    r1.f4 should equal (List("hello", "world"))
    r1.f5 should equal (List(NOW, NOW.plus(1000)))

    val r2 = RecordWithRepeatedPrimitives(Nil, Nil, Nil, Nil, Nil)
    r2.f1 should equal (Nil)
    r2.f2 should equal (Nil)
    r2.f3 should equal (Nil)
    r2.f4 should equal (Nil)
    r2.f5 should equal (Nil)
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "REQUIRED", "name": "f1", "type": "RECORD", "fields": [{"mode": "REQUIRED", "name": "g", "type": "INTEGER"}]},
      |    {"mode": "REQUIRED", "name": "f2", "type": "RECORD", "fields": [{"mode": "NULLABLE", "name": "g", "type": "INTEGER"}]},
      |    {"mode": "REQUIRED", "name": "f3", "type": "RECORD", "fields": [{"mode": "REPEATED", "name": "g", "type": "INTEGER"}]}
      |  ]
      |}
    """.stripMargin)
  class RecordWithRequiredRecords

  it should "support required records" in {
    val r = RecordWithRequiredRecords(f1(1L), f2(Some(1L)), f3(List(1L, 2L)))
    r.f1.g should equal (1L)
    r.f2.g should equal (Some(1L))
    r.f3.g should equal (List(1L, 2L))
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "NULLABLE", "name": "f1", "type": "RECORD", "fields": [{"mode": "REQUIRED", "name": "g", "type": "INTEGER"}]},
      |    {"mode": "NULLABLE", "name": "f2", "type": "RECORD", "fields": [{"mode": "NULLABLE", "name": "g", "type": "INTEGER"}]},
      |    {"mode": "NULLABLE", "name": "f3", "type": "RECORD", "fields": [{"mode": "REPEATED", "name": "g", "type": "INTEGER"}]}
      |  ]
      |}
    """.stripMargin)
  class RecordWithNullableRecords

  it should "support nullable records" in {
    val r = RecordWithNullableRecords(Some(f12(1L)), Some(f22(Some(1L))), Some(f32(List(1L, 2L))))
    r.f1.get.g should equal (1L)
    r.f2.get.g should equal (Some(1L))
    r.f3.get.g should equal (List(1L, 2L))
  }

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "REPEATED", "name": "f1", "type": "RECORD", "fields": [{"mode": "REQUIRED", "name": "g", "type": "INTEGER"}]},
      |    {"mode": "REPEATED", "name": "f2", "type": "RECORD", "fields": [{"mode": "NULLABLE", "name": "g", "type": "INTEGER"}]},
      |    {"mode": "REPEATED", "name": "f3", "type": "RECORD", "fields": [{"mode": "REPEATED", "name": "g", "type": "INTEGER"}]}
      |  ]
      |}
    """.stripMargin)
  class RecordWithRepeatedRecords

  it should "support repeated records" in {
    val r = RecordWithRepeatedRecords(List(f13(1L)), List(f23(Some(1L))), List(f33(List(1L, 2L))))
    r.f1 should equal (List(f13(1L)))
    r.f2 should equal (List(f23(Some(1L))))
    r.f3 should equal (List(f33(List(1L, 2L))))
  }

  @BigQueryType.toTable()
  case class ToTable(f1: Long, f2: Double, f3: Boolean, f4: String, f5: Instant)

  "BigQueryEntity.toTable" should "support .tupled in companion object" in {
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

}
