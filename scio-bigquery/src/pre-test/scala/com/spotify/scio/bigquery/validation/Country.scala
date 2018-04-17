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


class Country(val data: String) extends AnyVal

object Country {

  def apply(data: String): Country = {
    if (!isValid(data)) {
      throw new IllegalArgumentException("Not valid")
    }
    new Country(data)
  }

  def isValid(data: String): Boolean = data.length == 2

  def parse(data: String): Country = Country(data)

  def stringType: String = "COUNTRY"

  def bigQueryType: String = "STRING"

}
