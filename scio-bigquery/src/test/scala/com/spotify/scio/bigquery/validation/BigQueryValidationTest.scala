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


package com.spotify.scio.bigquery.validation

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.{TableRow, description}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

// This test shows how you can utilize the `SampleOverrideTypeProvider` to override types using
// properties on the individual field level processing data
class BigQueryValidationTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  @BigQueryType.fromSchema(
    """
      |{
      |  "fields":
      |  [ {"mode": "REQUIRED", "name": "country", "type": "STRING", "description": "COUNTRY"},
      |    {"mode": "REQUIRED", "name": "countryString", "type": "STRING"},
      |    {"mode": "REQUIRED", "name": "noCountry", "type": "STRING", "description": "NOCOUNTRY"}
      |  ]
      |}
    """.stripMargin)
  class CountryInput

  @BigQueryType.toTable
  case class CountryOutput(@description("COUNTRY") country: Country,
                           countryString: String,
                           @description("NOCOUNTRY") noCountry: String)

  "ValidationProvider" should "override types using SampleValidationProvider for fromSchema" in {
    val countryInput = CountryInput(Country("US"), "UK", "No Country")
    countryInput.country.data shouldBe "US"
    countryInput.countryString shouldBe "UK"
    countryInput.noCountry shouldBe "No Country"
  }

  "ValidationProvider" should "override types using SampleValidationProvider for toTable" in {
    CountryOutput.schema.getFields.get(0).getType shouldBe "STRING"
    CountryOutput.schema.getFields.get(1).getType shouldBe "STRING"
    CountryOutput.schema.getFields.get(2).getType shouldBe "STRING"
  }

  "ValidationProvider" should "properly validate data" in {
    assertThrows[IllegalArgumentException]{
      CountryInput(Country("USA"), "UK", "No Country")
    }
  }

  "ValidationProvider" should "throw an error when converting invalid data" in {
    assertThrows[IllegalArgumentException] {
      val tableRow = TableRow("country" -> "USA", "countryString" -> "USA", "noCountry" -> "USA")
      val input = CountryInput.fromTableRow(tableRow)
    }
  }

  "ValidationProvider" should "no error thrown converting valid data" in {
    val tableRow = TableRow("country" -> "US", "countryString" -> "USA", "noCountry" -> "USA")
    CountryInput.fromTableRow(tableRow)
  }
}
