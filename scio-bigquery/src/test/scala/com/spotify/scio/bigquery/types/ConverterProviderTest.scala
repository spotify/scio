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

import com.spotify.scio.bigquery._
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class ConverterProviderTest extends AnyFlatSpec with Matchers {
  import ConverterProviderTest._

  "ConverterProvider" should "throw NPE with meaningful message for null in REQUIRED field" in {
    the[NullPointerException] thrownBy {
      Required.fromTableRow(TableRow())
    } should have message """REQUIRED field "a" is null"""
  }

  it should "handle null in NULLABLE field" in {
    Nullable.fromTableRow(TableRow()) shouldBe Nullable(None)
  }

  it should "throw NPE with meaningful message for null in REPEATED field" in {
    the[NullPointerException] thrownBy {
      Repeated.fromTableRow(TableRow())
    } should have message """REPEATED field "a" is null"""
  }

  it should "handle required geography type" in {
    val wkt = "POINT (30 10)"
    RequiredGeo.fromTableRow(TableRow("a" -> wkt)) shouldBe RequiredGeo(Geography(wkt))
    BigQueryType.toTableRow[RequiredGeo](RequiredGeo(Geography(wkt))) shouldBe TableRow("a" -> wkt)
  }
}

object ConverterProviderTest {

  @BigQueryType.toTable
  case class RequiredGeo(a: Geography)

  @BigQueryType.toTable
  case class Required(a: String)

  @BigQueryType.toTable
  case class Nullable(a: Option[String])

  @BigQueryType.toTable
  case class Repeated(a: List[String])
}
