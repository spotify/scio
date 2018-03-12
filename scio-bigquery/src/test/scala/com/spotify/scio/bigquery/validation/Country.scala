package com.spotify.scio.bigquery.validation

class Country(data: String) {

  def getData: String = data
}

object Country {

  def apply(data: String): Country = {
    if (!isValid(data)) {
      throw new IllegalArgumentException("Not valid")
    }
    new Country(data)
  }


  def isValid(data: String): Boolean = data.length == 2

  def parse(data: String): Country = new Country(data)

  def semanticType: String = "COUNTRY"

  def bigQueryType: String = "STRING"

}
