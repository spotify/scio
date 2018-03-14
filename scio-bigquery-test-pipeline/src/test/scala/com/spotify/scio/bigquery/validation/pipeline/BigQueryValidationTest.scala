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
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class BigQueryValidationTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks  {


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

  "validation" should "actually validate" in {
    val countryInput = CountryInput(Country("US"), "UK", "No Country")
    countryInput.country.getData shouldBe "US"
    countryInput.countryString shouldBe "UK"
    countryInput.noCountry shouldBe "No Country"
  }
}
