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

package com.spotify.scio.bigquery.types

import com.spotify.scio.bigquery.BigQueryUtil
import org.scalatest.{FlatSpec, Matchers}

class BigDecimalConversionTest extends FlatSpec with Matchers {
  case class BigBoy(x: BigDecimal)

  "big decimal values" should "roundtrip if they fit NUMERIC scale" in {
    val before = BigBoy(BigDecimal("459592.59696"))
    val after = BigQueryType.fromTableRow[BigBoy](BigQueryType.toTableRow[BigBoy](before))

    after should equal(before)
  }

  "big decimal values" should "be rounded to BQ precision on write" in {
    // if type does not fit bq NUMERIC type, insertion query will fail
    // at least that's what seen with `bq load` for json data
    val original = "4288.1111111119"
    val rounded = "4288.111111112"
    val before = BigBoy(BigDecimal(original))
    val after = BigQueryType.fromTableRow[BigBoy](BigQueryType.toTableRow[BigBoy](before))

    after should equal(BigBoy(BigDecimal(rounded)))
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
