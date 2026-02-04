/*
 * Copyright 2026 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.avro

import com.spotify.scio.avro.types.Schemas.parseSchema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

@AvroType.toSchema
@doc("User record schema")
case class UserType(@doc("user name") name: String, @doc("user age") age: Int)

// We have to keep this test out of the `com.spotify.scio.avro.types` package to replicate the Scala
// issue outlined in https://github.com/spotify/scio/issues/5874
class AvroTypeTest extends AnyFlatSpec with Matchers {
  "AvroType" should "preserve doc and field-level annotations" in {
    val userSchema: String =
      s"""
         |{
         |  "type": "record",
         |  "namespace": "com.spotify.scio.avro",
         |  "doc": "User record schema",
         |  "name": "UserType",
         |  "fields": [
         |    { "name": "name", "type": "string", "doc": "user name"},
         |    { "name": "age", "type": "int", "doc": "user age"}
         |  ]
         |}
         |""".stripMargin

    AvroType[UserType].schema.toString shouldBe parseSchema(userSchema).toString
  }
}
