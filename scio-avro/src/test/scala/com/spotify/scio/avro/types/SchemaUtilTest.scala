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

// scalastyle:off line.size.limit
package com.spotify.scio.avro.types

import com.spotify.scio.avro.types.Schemas._
import com.spotify.scio.avro.types.Schemas.FieldMode._
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class SchemaUtilTest extends FlatSpec with Matchers {

  "toPrettyString()" should "support primitive types" in {
    SchemaUtil.toPrettyString1(parseSchema(s"${basicFields()}")) shouldBe
      "case class BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)"
  }

  it should "support optional primitive types" in {
    SchemaUtil.toPrettyString1(parseSchema(s"${basicFields(OPTIONAL)}")) shouldBe
      "case class OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)"
  }

  it should "support primitive type arrays" in {
    SchemaUtil.toPrettyString1(parseSchema(s"${basicFields(ARRAY)}")) shouldBe
      "case class ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])"
  }

  it should "support primitive type maps" in {
    SchemaUtil.toPrettyString1(parseSchema(s"${basicFields(MAP)}")) shouldBe
      "case class MapFields(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])"
  }

  it should "support nested records" in {
    SchemaUtil.toPrettyString1(parseSchema(recordFields())) shouldBe
      f"""
         |case class NestedFields(basic: NestedFields$$BasicFields, optional: NestedFields$$OptionalFields, array: NestedFields$$ArrayFields, map: NestedFields$$MapFields)
         |case class NestedFields$$BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
         |case class NestedFields$$OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
         |case class NestedFields$$ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
         |case class NestedFields$$MapFields(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])
       """.stripMargin.trim
  }

  it should "support nested optional records" in {
    SchemaUtil.toPrettyString1(parseSchema(recordFields(OPTIONAL))) shouldBe
      f"""
         |case class OptionalNestedFields(basic: Option[OptionalNestedFields$$BasicFields] = None, optional: Option[OptionalNestedFields$$OptionalFields] = None, array: Option[OptionalNestedFields$$ArrayFields] = None, map: Option[OptionalNestedFields$$MapFields] = None)
         |case class OptionalNestedFields$$BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
         |case class OptionalNestedFields$$OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
         |case class OptionalNestedFields$$ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
         |case class OptionalNestedFields$$MapFields(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])
       """.stripMargin.trim
  }

  it should "support nested record arrays" in {
    SchemaUtil.toPrettyString1(parseSchema(recordFields(ARRAY))) shouldBe
      f"""
         |case class ArrayNestedFields(basic: List[ArrayNestedFields$$BasicFields], optional: List[ArrayNestedFields$$OptionalFields], array: List[ArrayNestedFields$$ArrayFields], map: List[ArrayNestedFields$$MapFields])
         |case class ArrayNestedFields$$BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
         |case class ArrayNestedFields$$OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
         |case class ArrayNestedFields$$ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
         |case class ArrayNestedFields$$MapFields(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])
       """.stripMargin.trim
  }

  it should "support nested record maps" in {
    SchemaUtil.toPrettyString1(parseSchema(recordFields(MAP))) shouldBe
      f"""
         |case class MapNestedFields(basic: Map[String,MapNestedFields$$BasicFields], optional: Map[String,MapNestedFields$$OptionalFields], array: Map[String,MapNestedFields$$ArrayFields], map: Map[String,MapNestedFields$$MapFields])
         |case class MapNestedFields$$BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
         |case class MapNestedFields$$OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
         |case class MapNestedFields$$ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
         |case class MapNestedFields$$MapFields(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])
       """.stripMargin.trim
  }

  it should "support multiple levels of nested records" in {
    val schema = parseSchema("""
                               |{
                               |  "type" : "record",
                               |  "name" : "Record",
                               |  "fields" : [
                               |    { "name" : "level1",
                               |      "type" : {
                               |        "type": "record",
                               |        "name": "Level1",
                               |        "fields": [
                               |           { "name" : "level2",
                               |             "type" : {
                               |               "type": "record",
                               |               "name": "Level2",
                               |               "fields": [
                               |                 { "name" : "level3",
                               |                   "type" : {
                               |                     "type": "record",
                               |                     "name": "Level3",
                               |                     "fields": [
                               |                       { "name": "intF", "type": "int"}
                               |                     ]}
                               |                 }
                               |               ]}
                               |           }
                               |        ]}
                               |    }
                               |  ]
                               |}
                               |""".stripMargin)
    SchemaUtil.toPrettyString1(schema) shouldBe
      f"""
         |case class Record(level1: Record$$Level1)
         |case class Record$$Level1(level2: Record$$Level1$$Level2)
         |case class Record$$Level1$$Level2(level3: Record$$Level1$$Level2$$Level3)
         |case class Record$$Level1$$Level2$$Level3(intF: Int)
       """.stripMargin.trim
  }

  it should "support indent" in {
    SchemaUtil.toPrettyString1(parseSchema(s"${basicFields()}"), 2) shouldBe
      """
        |case class BasicFields(
        |  boolF: Boolean,
        |  intF: Int,
        |  longF: Long,
        |  floatF: Float,
        |  doubleF: Double,
        |  stringF: String,
        |  byteStringF: ByteString)
      """.stripMargin.trim
  }

  it should "support reserved words" in {
    val expectedFields = SchemaUtil.scalaReservedWords
      .map(e => s"`$e`").mkString(start = "", sep = ": Long, ", end = ": Long")
    val schema =
      Schema.createRecord("Row",
        null,
        null,
        false,
        SchemaUtil.scalaReservedWords.map {
          name =>
            new Schema.Field(name, Schema.create(Schema.Type.LONG), null, null.asInstanceOf[Any])
        }.asJava)
    SchemaUtil.toPrettyString1(schema) shouldBe s"case class Row($expectedFields)"
  }

}
// scalastyle:on line.size.limit
