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
package com.spotify.scio.schemas

import org.apache.beam.sdk.schemas.Schema.{Field, FieldType}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.collection.JavaConverters._
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.schemas.{Schema => BSchema, SchemaCoder}
import org.apache.beam.sdk.util.SerializableUtils.ensureSerializable
import scala.collection.mutable

final class SchemaMaterializerTest extends AnyFlatSpec with Matchers {
  "SchemaMaterializer" should "materialize correct FieldType" in {
    def fieldTypes[T](s: Schema[T]): List[Field] =
      SchemaMaterializer.materialize(s)._1.getFields().asScala.toList

    fieldTypes(Schema[Short]).headOption.map(_.getType) shouldBe Some(FieldType.INT16)
    fieldTypes(Schema[Int]).headOption.map(_.getType) shouldBe Some(FieldType.INT32)
    fieldTypes(Schema[Long]).headOption.map(_.getType) shouldBe Some(FieldType.INT64)
    fieldTypes(Schema[Float]).headOption.map(_.getType) shouldBe Some(FieldType.FLOAT)
    fieldTypes(Schema[Double]).headOption.map(_.getType) shouldBe Some(FieldType.DOUBLE)
    fieldTypes(Schema[Byte]).headOption.map(_.getType) shouldBe Some(FieldType.BYTE)
    fieldTypes(Schema[String]).headOption.map(_.getType) shouldBe Some(FieldType.STRING)
    fieldTypes(Schema[BigDecimal]).headOption.map(_.getType) shouldBe Some(FieldType.DECIMAL)
    fieldTypes(Schema[Boolean]).headOption.map(_.getType) shouldBe Some(FieldType.BOOLEAN)
    fieldTypes(Schema[Array[Byte]]).headOption.map(_.getType) shouldBe Some(FieldType.BYTES)
    fieldTypes(Schema[Array[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )
    fieldTypes(Schema[Option[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.STRING.withNullable(true)
    )
    fieldTypes(Schema[List[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )
    fieldTypes(Schema[Map[String, String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.map(FieldType.STRING, FieldType.STRING)
    )

    fieldTypes(Schema[org.joda.time.Instant]).headOption.map(_.getType) shouldBe Some(
      FieldType.DATETIME
    )
    fieldTypes(Schema[org.joda.time.DateTime]).headOption.map(_.getType) shouldBe Some(
      FieldType.DATETIME
    )

    fieldTypes(Schema[java.lang.Short]).headOption.map(_.getType) shouldBe Some(FieldType.INT16)
    fieldTypes(Schema[java.lang.Integer]).headOption.map(_.getType) shouldBe Some(FieldType.INT32)
    fieldTypes(Schema[java.lang.Long]).headOption.map(_.getType) shouldBe Some(FieldType.INT64)
    fieldTypes(Schema[java.lang.Float]).headOption.map(_.getType) shouldBe Some(FieldType.FLOAT)
    fieldTypes(Schema[java.lang.Double]).headOption.map(_.getType) shouldBe Some(FieldType.DOUBLE)
    fieldTypes(Schema[java.lang.Byte]).headOption.map(_.getType) shouldBe Some(FieldType.BYTE)
    fieldTypes(Schema[java.lang.String]).headOption.map(_.getType) shouldBe Some(FieldType.STRING)
    fieldTypes(Schema[java.math.BigDecimal]).headOption.map(_.getType) shouldBe Some(
      FieldType.DECIMAL
    )
    fieldTypes(Schema[java.lang.Boolean]).headOption.map(_.getType) shouldBe Some(FieldType.BOOLEAN)
    fieldTypes(Schema[java.util.List[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )
    fieldTypes(Schema[java.util.ArrayList[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )
    fieldTypes(Schema[java.util.Map[String, String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.map(FieldType.STRING, FieldType.STRING)
    )

    // More Collections
    fieldTypes(Schema[Set[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )

    fieldTypes(Schema[TraversableOnce[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )

    fieldTypes(Schema[mutable.ArrayBuffer[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )

    fieldTypes(Schema[mutable.Set[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )

    fieldTypes(Schema[Vector[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )

    fieldTypes(Schema[mutable.ArrayBuffer[String]]).headOption.map(_.getType) shouldBe Some(
      FieldType.array(FieldType.STRING)
    )
  }

  it should "support logical types" in {
    import java.net.URI
    val uriSchema = LogicalType[URI, String](
      org.apache.beam.sdk.schemas.Schema.FieldType.STRING,
      _.toString,
      s => new URI(s)
    )
    val (schema, toRow, fromRow) = SchemaMaterializer.materialize[URI](uriSchema)
    val uri = URI.create("https://spotify.com")
    val row = Row.withSchema(schema).addValue(uri.toString).build()
    toRow(uri) shouldBe row
    fromRow(toRow(uri)) shouldBe uri
  }

  it should "Generate serializable Schemas and SchemaCoders" in {
    import java.util.Locale

    case class Bar(s: String, x: Int)
    case class Foo(a: String, b: Option[Bar])
    case class Baz(a: Foo, b: Locale)

    implicit val localeSchema: Schema[Locale] =
      LogicalType[Locale, String](
        BSchema.FieldType.STRING,
        l => l.toLanguageTag(),
        s => Locale.forLanguageTag(s)
      )

    val schemas =
      List(
        SchemaMaterializer.beamSchema[Locale],
        SchemaMaterializer.beamSchema[Foo],
        SchemaMaterializer.beamSchema[Baz],
        SchemaMaterializer.beamSchema[List[Foo]],
        SchemaMaterializer.beamSchema[Map[String, Foo]]
      )

    for (s <- schemas) {
      ensureSerializable(s)
      // Coerce to Serializable to skip coder equality check
      val coder: java.io.Serializable = SchemaCoder.of(s)
      ensureSerializable(coder)
    }
  }

  it should "Support Optional fields when reading a Row" in {
    case class Bar(s: String, x: Int)
    case class Foo(a: String, b: Option[Bar])
    val (schema, _, from) = SchemaMaterializer.materialize[Foo](Schema[Foo])
    val row =
      Row
        .withSchema(schema)
        .addValue("Hello")
        .addValue(null)
        .build()
    from(row) shouldBe Foo("Hello", None)
  }
}
