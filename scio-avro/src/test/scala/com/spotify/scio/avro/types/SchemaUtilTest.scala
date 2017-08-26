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
    SchemaUtil.toPrettyString("BasicRecord", parseSchema(s"${basicFields()}")) should equal (
      """
        |case class BasicRecord(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
      """.stripMargin.trim)
  }

  it should "support optional primitive types" in {
    SchemaUtil.toPrettyString("OptionalRecord", parseSchema(s"${basicFields(OPTIONAL)}")) should equal (
      """
        |case class OptionalRecord(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
      """.stripMargin.trim)
  }

  it should "support primitive type arrays" in {
    SchemaUtil.toPrettyString("ArrayRecord", parseSchema(s"${basicFields(ARRAY)}")) should equal (
      """
        |case class ArrayRecord(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
      """.stripMargin.trim)
  }

  it should "support primitive type maps" in {
    SchemaUtil.toPrettyString("MapRecord", parseSchema(s"${basicFields(MAP)}")) should equal (
      """
        |case class MapRecord(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])
      """.stripMargin.trim)
  }

  it should "support nested records" in {
    SchemaUtil.toPrettyString("NestedRecord", parseSchema(recordFields())) should equal (
      """
        |case class NestedRecord(basic: NestedRecord$BasicFields, optional: NestedRecord$OptionalFields, array: NestedRecord$ArrayFields, map: NestedRecord$MapFields)
        |case class NestedRecord$BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
        |case class NestedRecord$OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
        |case class NestedRecord$ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
        |case class NestedRecord$MapFields(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])
      """.stripMargin.trim)
  }

  it should "support nested optional records" in {
    SchemaUtil.toPrettyString("OptionalNestedRecord", parseSchema(recordFields(OPTIONAL))) should equal (
      """
        |case class OptionalNestedRecord(basic: Option[OptionalNestedRecord$BasicFields] = None, optional: Option[OptionalNestedRecord$OptionalFields] = None, array: Option[OptionalNestedRecord$ArrayFields] = None, map: Option[OptionalNestedRecord$MapFields] = None)
        |case class OptionalNestedRecord$BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
        |case class OptionalNestedRecord$OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
        |case class OptionalNestedRecord$ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
        |case class OptionalNestedRecord$MapFields(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])
      """.stripMargin.trim)
  }

  it should "support nested record arrays" in {
    SchemaUtil.toPrettyString("ArrayNestedRecord", parseSchema(recordFields(ARRAY))) should equal (
      """
        |case class ArrayNestedRecord(basic: List[ArrayNestedRecord$BasicFields], optional: List[ArrayNestedRecord$OptionalFields], array: List[ArrayNestedRecord$ArrayFields], map: List[ArrayNestedRecord$MapFields])
        |case class ArrayNestedRecord$BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
        |case class ArrayNestedRecord$OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
        |case class ArrayNestedRecord$ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
        |case class ArrayNestedRecord$MapFields(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])
      """.stripMargin.trim)
  }

  it should "support nested record maps" in {
    SchemaUtil.toPrettyString("MapNestedRecord", parseSchema(recordFields(MAP))) should equal (
      """
        |case class MapNestedRecord(basic: Map[String,MapNestedRecord$BasicFields], optional: Map[String,MapNestedRecord$OptionalFields], array: Map[String,MapNestedRecord$ArrayFields], map: Map[String,MapNestedRecord$MapFields])
        |case class MapNestedRecord$BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
        |case class MapNestedRecord$OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
        |case class MapNestedRecord$ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
        |case class MapNestedRecord$MapFields(boolF: Map[String,Boolean], intF: Map[String,Int], longF: Map[String,Long], floatF: Map[String,Float], doubleF: Map[String,Double], stringF: Map[String,String], byteStringF: Map[String,ByteString])
      """.stripMargin.trim)
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
    SchemaUtil.toPrettyString("Record", schema) should equal (
      """
        |case class Record(level1: Record$Level1)
        |case class Record$Level1(level2: Record$Level1$Level2)
        |case class Record$Level1$Level2(level3: Record$Level1$Level2$Level3)
        |case class Record$Level1$Level2$Level3(intF: Int)
      """.stripMargin.trim)
  }

  it should "support indent" in {
    SchemaUtil.toPrettyString("BasicRecord", parseSchema(s"${basicFields()}"), 2) should equal (
      """
        |case class BasicRecord(
        |  boolF: Boolean,
        |  intF: Int,
        |  longF: Long,
        |  floatF: Float,
        |  doubleF: Double,
        |  stringF: String,
        |  byteStringF: ByteString)
      """.stripMargin.trim)
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
    SchemaUtil.toPrettyString("Record", schema) should equal (
      s"""|case class Record($expectedFields)""".stripMargin
    )
  }

}
// scalastyle:on line.size.limit
