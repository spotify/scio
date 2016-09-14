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

package com.spotify.scio.bigquery.types

import com.google.api.services.bigquery.model.TableRow
import org.scalatest.{FlatSpec, Matchers}

/**
 * Tests for scala 2.11
 * Specifically - scala 2.11 removes 22 fields limit on case classes which we use in BQ macros
 */
class TypeProviderTestScala211 extends FlatSpec with Matchers {
  @BigQueryType.fromSchema(
    """
      |{
      |  "fields": [
      |    {"mode": "REQUIRED", "name": "f1", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f2", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f3", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f4", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f5", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f6", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f7", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f8", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f9", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f10", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f11", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f12", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f13", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f14", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f15", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f16", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f17", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f18", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f19", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f20", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f21", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f22", "type": "INTEGER"},
      |    {"mode": "REQUIRED", "name": "f23", "type": "INTEGER"}
      |  ]
      |}
    """.stripMargin)
  class ArtisanalMoreThan22Fields

  "BigQueryType.fromSchema" should "support .schema in companion object with more than 22 fields" in {
    ArtisanalMoreThan22Fields(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
    ArtisanalMoreThan22Fields.schema should not be null
  }

  it should "not provide .tupled in companion object with more than 22 fields" in {
    ArtisanalMoreThan22Fields.getClass.getMethods.map(_.getName) should not contain "tupled"
  }

  @BigQueryType.toTable
  case class TwentyThree(a1:Int,a2:Int,a3:Int,a4:Int,a5:Int,a6:Int,a7:Int,a8:Int,a9:Int,a10:Int,
                         a11:Int,a12:Int,a13:Int,a14:Int,a15:Int,a16:Int,a17:Int,a18:Int,a19:Int,
                         a20:Int,a21:Int,a22:Int,a23:Int)

  "BigQueryType.toTable" should "not provide .tupled in companion object with more than 22 fields" in {
    TwentyThree.getClass.getMethods.map(_.getName) should not contain "tupled"
  }

  it should "support .schema in companion object with more than 22 fields" in {
    TwentyThree.schema should not be null
  }

  it should "support .fromTableRow in companion object" in {
    (classOf[(TableRow => TwentyThree)] isAssignableFrom TwentyThree.fromTableRow.getClass) shouldBe true
  }

  it should "support .toTableRow in companion object" in {
    (classOf[(TwentyThree => TableRow)] isAssignableFrom TwentyThree.toTableRow.getClass) shouldBe true
  }
}
