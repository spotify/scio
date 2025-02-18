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

import com.google.protobuf.ByteString
import com.spotify.scio.bigquery._
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.Schema
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import scala.jdk.CollectionConverters._

class ConverterProviderTest extends AnyFlatSpec with Matchers {
  import ConverterProviderTest._

  "ConverterProvider" should "throw NoSuchElementException with meaningful message for missing REQUIRED field" in {
    the[NoSuchElementException] thrownBy {
      Required.fromTableRow(TableRow())
    } should have message "Field not found: a"
  }

  it should "handle null in NULLABLE field" in {
    Nullable.fromTableRow(TableRow()) shouldBe Nullable(None)
  }

  it should "throw NoSuchElementException with meaningful message for missing in REPEATED field" in {
    the[NoSuchElementException] thrownBy {
      Repeated.fromTableRow(TableRow())
    } should have message "Field not found: a"
  }

  it should "handle required geography type" in {
    val wkt = "POINT (30 10)"
    RequiredGeo.fromTableRow(TableRow("a" -> wkt)) shouldBe RequiredGeo(Geography(wkt))
    BigQueryType.toTableRow[RequiredGeo](RequiredGeo(Geography(wkt))) shouldBe TableRow("a" -> wkt)
  }

  it should "handle required json type" in {
    val wkt = """{"name":"Alice","age":30}"""

    RequiredJson.fromTableRow(TableRow("a" -> wkt)) shouldBe RequiredJson(Json(wkt))
    BigQueryType.toTableRow[RequiredJson](RequiredJson(Json(wkt))) shouldBe TableRow("a" -> wkt)
  }

  it should "handle required big numeric type" in {
    val bigNumeric = "12.34567890123456789012345678901234567890"
    val wkt = BigDecimal(bigNumeric)
    RequiredBigNumeric.fromTableRow(TableRow("a" -> bigNumeric)) shouldBe RequiredBigNumeric(
      BigNumeric(wkt)
    )
    BigQueryType.toTableRow(RequiredBigNumeric(BigNumeric(wkt))) shouldBe TableRow(
      "a" -> bigNumeric
    )
  }

  it should "handle case classes with methods" in {
    RequiredWithMethod.fromTableRow(TableRow("a" -> "")) shouldBe RequiredWithMethod("")
    BigQueryType.toTableRow[RequiredWithMethod](RequiredWithMethod("")) shouldBe TableRow("a" -> "")
  }

  it should "convert to stable types for the coder" in {
    import com.spotify.scio.testing.CoderAssertions._
    // Coder[TableRow] is destructive
    // make sure the target TableRow format chosen by the BigQueryType conversion is stable
    AllTypes.toTableRow(AllTypes()) coderShould roundtrip()
  }

  it should "convert nested case classes to and from Avro" in {
    val bqt = BigQueryType[CaseClassWithNested]

    val expectedSchema = new Schema.Parser().parse(s"""{
         |  "type":"record",
         |  "name":"CaseClassWithNested",
         |  "namespace":"org.apache.beam.sdk.io.gcp.bigquery",
         |  "doc":"Translated Avro Schema for com.spotify.scio.bigquery.types.ConverterProviderTest.CaseClassWithNested",
         |  "fields":[
         |    {
         |      "name": "requiredField",
         |      "type": {
         |        "type": "record",
         |        "name": "requiredField",
         |        "doc": "Translated Avro Schema for requiredField",
         |        "fields": [{"name": "a" , "type": "string"}]}
         |      },
         |      {
         |        "name": "optionalField",
         |        "type":[
         |          "null",
         |          {
         |            "type": "record",
         |            "name": "optionalField",
         |            "doc": "Translated Avro Schema for optionalField",
         |            "fields": [{"name": "a", "type": "string"}]
         |          }]
         |      },
         |      {
         |        "name": "repeatedField",
         |        "type": {
         |          "type": "array",
         |          "items":{
         |            "type": "record",
         |            "name": "repeatedField",
         |            "doc": "Translated Avro Schema for repeatedField",
         |            "fields": [{"name": "a", "type": "string"}]
         |          }}}]}
         |""".stripMargin)

    SchemaProvider.avroSchemaOf[CaseClassWithNested] shouldBe expectedSchema

    val cc1 = CaseClassWithNested(Required("foo"), Some(Required("bar")), List(Required("baz")))
    val avro1 = new GenericRecordBuilder(expectedSchema)
      .set(
        "requiredField",
        new GenericRecordBuilder(expectedSchema.getField("requiredField").schema())
          .set("a", "foo")
          .build()
      )
      .set(
        "optionalField",
        new GenericRecordBuilder(expectedSchema.getField("optionalField").schema().getTypes.get(1))
          .set("a", "bar")
          .build()
      )
      .set(
        "repeatedField",
        List(
          new GenericRecordBuilder(expectedSchema.getField("repeatedField").schema().getElementType)
            .set("a", "baz")
            .build()
        ).asJava
      )
      .build()

    bqt.toAvro(cc1) shouldBe avro1
    bqt.fromAvro(avro1) shouldBe cc1
  }
}

object ConverterProviderTest {

  @BigQueryType.toTable
  case class RequiredGeo(a: Geography)

  @BigQueryType.toTable
  case class RequiredJson(a: Json)

  @BigQueryType.toTable
  case class RequiredBigNumeric(a: BigNumeric)

  @BigQueryType.toTable
  case class Required(a: String)

  @BigQueryType.toTable
  case class Nullable(a: Option[String])

  @BigQueryType.toTable
  case class Repeated(a: List[String])

  @BigQueryType.toTable
  case class RequiredWithMethod(a: String) {
    val caseClassPublicValue: String = ""
    def accessorMethod: String = ""
    def method(x: String): String = x
  }

  @BigQueryType.toTable
  case class AllTypes(
    bool: Boolean = true,
    int: Int = 1,
    long: Long = 2L,
    float: Float = 3.3f,
    double: Double = 4.4,
    numeric: BigDecimal = BigDecimal(5),
    string: String = "6",
    byteString: ByteString = ByteString.copyFromUtf8("7"),
    timestamp: Instant = Instant.now(),
    date: LocalDate = LocalDate.now(),
    time: LocalTime = LocalTime.now(),
    datetime: LocalDateTime = LocalDateTime.now(),
    geography: Geography = Geography("POINT (8 8)"),
    json: Json = Json("""{"key": 9,"value": 10}"""),
    bigNumeric: BigNumeric = BigNumeric(BigDecimal(11))
  )

  @BigQueryType.toTable()
  case class CaseClassWithNested(
    requiredField: Required,
    optionalField: Option[Required],
    repeatedField: List[Required]
  )
}
