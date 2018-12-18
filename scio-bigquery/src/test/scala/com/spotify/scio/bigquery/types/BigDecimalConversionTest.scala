package com.spotify.scio.bigquery.types

import java.math.MathContext

import com.spotify.scio.bigquery.BigQueryUtil
import org.scalatest.{FlatSpec, Matchers}

class BigDecimalConversionTest extends FlatSpec with Matchers {
  case class BigBoy(x: BigDecimal)

  "big decimal values" should "roundtrip if they fit NUMERIC" in {
    val before = BigBoy(BigDecimal("459592.59696"))
    val after = BigQueryType.fromTableRow[BigBoy](BigQueryType.toTableRow[BigBoy](before))

    after should equal(before)
  }

  "big decimal values" should "be rounded to BQ precision on write" in {
    // if type does not fit bq NUMERIC type, insertion query will fail
    // at least that's what I see with `bq load` for json data
    val original = "4288.1111111119"
    val rounded = "4288.111111112"
    val before = BigBoy(BigDecimal(original))
    val after = BigQueryType.fromTableRow[BigBoy](BigQueryType.toTableRow[BigBoy](before))

    after should equal(BigBoy(BigDecimal(rounded)))
  }

  "math context" should "be identical to BQ settings" in {
    // math context is a set of rules on how to do the rounding of bigdecimal value
    // it can't be encoded in NUMERIC, so it will be lost during conversion
    // the most sensible return value for math context is to mimic BQ behaviour
    // see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
    // and https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators
    val before = BigBoy(BigDecimal("4288.29392", MathContext.UNLIMITED))
    val bqRow = BigQueryType.toTableRow[BigBoy](before)
    val after = BigQueryType.fromTableRow[BigBoy](bqRow)
    val bqMath = new MathContext(38)
    after.x.mc should equal(bqMath)
  }

  "bigdecimal" should "be represented as NUMERIC in schema" in {
    val expectedRawSchema =
      """
        |{"mode": "REQUIRED", "name": "x", "type": "NUMERIC"}
        |""".stripMargin
    val expectedParsedSchema = BigQueryUtil.parseSchema(expectedRawSchema)
    val actualFields = BigQueryType.schemaOf[BigBoy].getFields
    actualFields.get(0) shouldBe (expectedParsedSchema)
  }

}
