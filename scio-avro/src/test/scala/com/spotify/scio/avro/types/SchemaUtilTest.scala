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
    SchemaUtil.toPrettyString(parseSchema(s"${basicFields()}")) should equal (
      """
        |case class BasicFields(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
      """.stripMargin.trim)
  }

  it should "support optional primitive types" in {
    SchemaUtil.toPrettyString(parseSchema(s"${basicFields(OPTIONAL)}")) should equal (
      """
        |case class OptionalFields(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
      """.stripMargin.trim)
  }

  it should "support primitive type arrays" in {
    SchemaUtil.toPrettyString(parseSchema(s"${basicFields(ARRAY)}")) should equal (
      """
        |case class ArrayFields(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
      """.stripMargin.trim)
  }

  it should "support primitive type maps" in {
    SchemaUtil.toPrettyString(parseSchema(s"${basicFields(MAP)}")) should equal (
      """
        |case class MapFields(boolF: Map[String, Boolean], intF: Map[String, Int], longF: Map[String, Long], floatF: Map[String, Float], doubleF: Map[String, Double], stringF: Map[String, String], byteStringF: Map[String, ByteString])
      """.stripMargin.trim)
  }

  it should "support nested records" in {
    NameProvider.reset()
    SchemaUtil.toPrettyString(parseSchema(recordFields())) should equal (
      """
        |case class NestedFields(basic: BasicFields$1, optional: OptionalFields$1, array: ArrayFields$1, map: MapFields$1)
        |case class BasicFields$1(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
        |case class OptionalFields$1(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
        |case class ArrayFields$1(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
        |case class MapFields$1(boolF: Map[String, Boolean], intF: Map[String, Int], longF: Map[String, Long], floatF: Map[String, Float], doubleF: Map[String, Double], stringF: Map[String, String], byteStringF: Map[String, ByteString])
      """.stripMargin.trim)
  }

  it should "support nested optional records" in {
    NameProvider.reset()
    SchemaUtil.toPrettyString(parseSchema(recordFields(OPTIONAL))) should equal (
      """
        |case class OptionalNestedFields(basic: Option[BasicFields$1] = None, optional: Option[OptionalFields$1] = None, array: Option[ArrayFields$1] = None, map: Option[MapFields$1] = None)
        |case class BasicFields$1(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
        |case class OptionalFields$1(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
        |case class ArrayFields$1(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
        |case class MapFields$1(boolF: Map[String, Boolean], intF: Map[String, Int], longF: Map[String, Long], floatF: Map[String, Float], doubleF: Map[String, Double], stringF: Map[String, String], byteStringF: Map[String, ByteString])
      """.stripMargin.trim)
  }

  it should "support nested record arrays" in {
    NameProvider.reset()
    SchemaUtil.toPrettyString(parseSchema(recordFields(ARRAY))) should equal (
      """
        |case class ArrayNestedFields(basic: List[BasicFields$1], optional: List[OptionalFields$1], array: List[ArrayFields$1], map: List[MapFields$1])
        |case class BasicFields$1(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
        |case class OptionalFields$1(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
        |case class ArrayFields$1(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
        |case class MapFields$1(boolF: Map[String, Boolean], intF: Map[String, Int], longF: Map[String, Long], floatF: Map[String, Float], doubleF: Map[String, Double], stringF: Map[String, String], byteStringF: Map[String, ByteString])
      """.stripMargin.trim)
  }

  it should "support nested record maps" in {
    NameProvider.reset()
    SchemaUtil.toPrettyString(parseSchema(recordFields(MAP))) should equal (
      """
        |case class MapNestedFields(basic: Map[String, BasicFields$1], optional: Map[String, OptionalFields$1], array: Map[String, ArrayFields$1], map: Map[String, MapFields$1])
        |case class BasicFields$1(boolF: Boolean, intF: Int, longF: Long, floatF: Float, doubleF: Double, stringF: String, byteStringF: ByteString)
        |case class OptionalFields$1(boolF: Option[Boolean] = None, intF: Option[Int] = None, longF: Option[Long] = None, floatF: Option[Float] = None, doubleF: Option[Double] = None, stringF: Option[String] = None, byteStringF: Option[ByteString] = None)
        |case class ArrayFields$1(boolF: List[Boolean], intF: List[Int], longF: List[Long], floatF: List[Float], doubleF: List[Double], stringF: List[String], byteStringF: List[ByteString])
        |case class MapFields$1(boolF: Map[String, Boolean], intF: Map[String, Int], longF: Map[String, Long], floatF: Map[String, Float], doubleF: Map[String, Double], stringF: Map[String, String], byteStringF: Map[String, ByteString])
      """.stripMargin.trim)
  }

  it should "support indent" in {
    SchemaUtil.toPrettyString(parseSchema(s"${basicFields()}"), 2) should equal (
      """
        |case class BasicFields(
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
    SchemaUtil.toPrettyString(schema) should equal (
      s"""|case class Row($expectedFields)""".stripMargin
    )
  }

}
// scalastyle:on line.size.limit
