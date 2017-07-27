/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.avro.types

import com.spotify.scio.avro.types.Schemas._
import com.spotify.scio.avro.types.Schemas.FieldMode._
import org.scalatest.{FlatSpec, Matchers}

class SchemaProviderTest extends FlatSpec with Matchers {
  "SchemaProvider.toSchema" should "support primitive types" in {
    SchemaProvider.schemaOf[BasicFields] shouldBe
      parseSchema(s"${basicFields()}")
  }

  it should "support nullable primitive types" in {
    SchemaProvider.schemaOf[OptionalFields] shouldBe
      parseSchema(s"${basicFields(OPTIONAL)}")
  }

  it should "support arrays of primitive types" in {
    SchemaProvider.schemaOf[ArrayFields] shouldBe
      parseSchema(s"${basicFields(ARRAY)}")
  }

  it should "support maps of primitive types" in {
    SchemaProvider.schemaOf[MapFields] shouldBe
      parseSchema(s"${basicFields(MAP)}")
  }

  it should "support basic records" in {
    SchemaProvider.schemaOf[NestedFields] shouldBe
      parseSchema(recordFields())
  }

  it should "support optional records" in {
    SchemaProvider.schemaOf[OptionalNestedFields] shouldBe
      parseSchema(recordFields(OPTIONAL))
  }

  it should "support array of records" in {
    SchemaProvider.schemaOf[ArrayNestedFields] shouldBe
      parseSchema(recordFields(ARRAY))
  }

  it should "support map of records" in {
    SchemaProvider.schemaOf[MapNestedFields] shouldBe
      parseSchema(recordFields(MAP))
  }

  @doc("User record schema")
  case class User(@doc("user name") name: String, @doc("user age") age: Int)
  @doc("Account record schema")
  case class Account(@doc("account user") user: User,
                     @doc("in USD") balance: Double)

  val userSchema =
    s"""
       |{
       |  "type": "record",
       |  "namespace": "com.spotify.scio.avro.types.SchemaProviderTest",
       |  "doc": "User record schema",
       |  "name": "User",
       |  "fields": [
       |    { "name": "name", "type": "string", "doc": "user name"},
       |    { "name": "age", "type": "int", "doc": "user age"}
       |  ]
       |}
       |""".stripMargin

  val accountSchema =
    s"""
       |{
       |  "type": "record",
       |  "namespace": "com.spotify.scio.avro.types.SchemaProviderTest",
       |  "doc": "Account record schema",
       |  "name": "Account",
       |  "fields": [
       |    { "name": "user", "type": $userSchema, "doc": "account user"},
       |    { "name": "balance", "type": "double", "doc": "in USD"}
       |  ]
       |}
     """.stripMargin

  it should "support doc annotation" in {
    // Schema.equals() ignores doc property.
    // Hence we need to turn Schemas to string in order to compare them.
    SchemaProvider.schemaOf[User].toString shouldBe parseSchema(userSchema).toString
    SchemaProvider.schemaOf[Account].toString shouldBe parseSchema(accountSchema).toString
  }
}
