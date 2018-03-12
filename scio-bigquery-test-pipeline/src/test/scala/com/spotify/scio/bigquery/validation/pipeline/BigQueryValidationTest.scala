package com.spotify.scio.bigquery.validation.pipeline

import com.spotify.scio.bigquery.types.BigQueryType
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import com.spotify.scio.bigquery.validation.Country

class BigQueryValidationTest extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks  {


  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [ {"mode": "REQUIRED", "name": "country", "type": "STRING", "description": "COUNTRY"},
      |              {"mode": "REQUIRED", "name": "countryString", "type": "STRING"},
      |              {"mode": "REQUIRED", "name": "noCountry", "type": "STRING", "description": "NOCOUNTRY"} ]
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
