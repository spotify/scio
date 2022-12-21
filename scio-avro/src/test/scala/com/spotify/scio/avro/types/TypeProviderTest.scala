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

package com.spotify.scio.avro.types

import com.google.protobuf.ByteString
import com.spotify.scio.avro.types.AvroType.HasAvroDoc
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._

class TypeProviderTest extends AnyFlatSpec with Matchers {
  @AvroType.toSchema
  case class ToSchema(
    boolF: Boolean,
    intF: Int,
    longF: Long,
    floatF: Float,
    doubleF: Double,
    stringF: String,
    bytesF: ByteString
  )

  "AvroType.toSchema" should "support .tupled in companion object" in {
    val r1 = ToSchema(true, 1, 2L, 1.5f, 2.5, "string", ByteString.copyFromUtf8("bytes"))
    val r2 = ToSchema.tupled((true, 1, 2L, 1.5f, 2.5, "string", ByteString.copyFromUtf8("bytes")))
    r1 shouldBe r2
  }

  it should "returns correct schema for plain objects" in {
    ToSchema.schema should be
    new Schema.Parser()
      .parse("""
                                  |{
                                  |  "type": "record",
                                  |  "namespace": "com.spotify.scio.avro.types.TypeProviderTest",
                                  |  "name": "ToSchema",
                                  |  "fields": [
                                  |    { "name": "boolF", "type": "boolean"},
                                  |    { "name": "intF", "type": "int"},
                                  |    { "name": "longF", "type": "long"},
                                  |    { "name": "floatF", "type": "float"},
                                  |    { "name": "doubleF", "type": "double"},
                                  |    { "name": "stringF", "type": "string"},
                                  |    { "name": "byteStringF", "type": "bytes"}
                                  |  ]
                                  |}
                                """.stripMargin)
  }

  it should "support .fromGenericRecord in companion object" in {
    (classOf[GenericRecord => ToSchema] isAssignableFrom
      ToSchema.fromGenericRecord.getClass) shouldBe true
  }

  it should "support .toGenericRecord in companion object" in {
    (classOf[ToSchema => GenericRecord] isAssignableFrom
      ToSchema.toGenericRecord.getClass) shouldBe true
  }

  it should "support round trip conversion from GenericRecord" in {
    val r1 = ToSchema(true, 1, 2L, 1.5f, 2.5, "string", ByteString.copyFromUtf8("bytes"))
    val r2 = ToSchema.fromGenericRecord(ToSchema.toGenericRecord(r1))
    r1 shouldBe r2
  }

  it should "create companion object that is a Function subtype" in {
    val cls = classOf[Function7[Boolean, Int, Long, Float, Double, String, ByteString, ToSchema]]
    (cls isAssignableFrom ToSchema.getClass) shouldBe true
  }

  it should "create companion object that is functionally equal to its apply method" in {
    def doApply(
      f: (Boolean, Int, Long, Float, Double, String, ByteString) => ToSchema
    )(x: (Boolean, Int, Long, Float, Double, String, ByteString)): ToSchema =
      f(x._1, x._2, x._3, x._4, x._5, x._6, x._7)

    val bytes = ByteString.copyFromUtf8("bytes")

    val expected1 = doApply(ToSchema)((true, 1, 2L, 1.5f, 2.5, "string", bytes))
    doApply(ToSchema.apply _)((true, 1, 2L, 1.5f, 2.5, "string", bytes)) shouldBe expected1

    val expected2 = ToSchema(true, 1, 2L, 1.5f, 2.5, "string", bytes)
    doApply(ToSchema)((true, 1, 2L, 1.5f, 2.5, "string", bytes)) shouldBe expected2
  }

  @AvroType.toSchema
  case class ReservedName(`type`: Boolean, `int`: Int)

  it should "returns correct schema for objects containing fields named as scala reserved word" in {
    ReservedName.schema should be
    new Schema.Parser()
      .parse("""
                                |{
                                |  "type": "record",
                                |  "namespace": "com.spotify.scio.avro.types.TypeProviderTest",
                                |  "name": "ReservedName",
                                |  "fields": [
                                |    { "name": "type", "type": "boolean"},
                                |    { "name": "int", "type": "int"}
                                |  ]
                                |}
                              """.stripMargin)
  }

  @AvroType.toSchema
  case class Artisanal1FieldToSchema(f1: Long)

  it should "not provide .tupled in companion object with single field" in {
    Artisanal1FieldToSchema.getClass.getMethods
      .map(_.getName) should not contain "tupled"
  }

  it should "create companion object that is a Function subtype for single field record" in {
    val cls = classOf[Function1[Long, Artisanal1FieldToSchema]]
    (cls isAssignableFrom Artisanal1FieldToSchema.getClass) shouldBe true
  }

  @AvroType.toSchema
  case class RecordWithDefault(x: Int, y: Int = 2)

  it should "work for case class with default params" in {
    classOf[Function2[Int, Int, RecordWithDefault]] isAssignableFrom RecordWithDefault.getClass
  }

  it should "support default argument correctly" in {
    RecordWithDefault(10).y shouldBe 2
  }

  @AvroType.toSchema
  case class TwentyThree(
    a1: Int,
    a2: Int,
    a3: Int,
    a4: Int,
    a5: Int,
    a6: Int,
    a7: Int,
    a8: Int,
    a9: Int,
    a10: Int,
    a11: Int,
    a12: Int,
    a13: Int,
    a14: Int,
    a15: Int,
    a16: Int,
    a17: Int,
    a18: Int,
    a19: Int,
    a20: Int,
    a21: Int,
    a22: Int,
    a23: Int
  )

  "AvroType.toSchema" should "not provide .tupled in companion object with >22 fields" in {
    TwentyThree.getClass.getMethods.map(_.getName) should not contain "tupled"
  }

  it should "support .schema in companion object with >22 fields" in {
    TwentyThree.schema should not be null
  }

  it should "support .fromGenericRecord in companion object with >22 fields" in {
    val cls = classOf[GenericRecord => TwentyThree]
    (cls isAssignableFrom TwentyThree.fromGenericRecord.getClass) shouldBe true
  }

  it should "support .toGenericRecord in companion object with >22 fields" in {
    val cls = classOf[TwentyThree => GenericRecord]
    (cls isAssignableFrom TwentyThree.toGenericRecord.getClass) shouldBe true
  }

  @AvroType.toSchema
  @doc("Avro doc")
  case class DocumentedRecord(a1: Int)

  it should "support table description" in {
    DocumentedRecord.doc shouldBe "Avro doc"
    DocumentedRecord.isInstanceOf[HasAvroDoc] shouldBe true
  }

  class Annotation1 extends StaticAnnotation
  class Annotation2 extends StaticAnnotation

  @Annotation1
  @AvroType.toSchema
  @Annotation2
  case class RecordWithSurroundingAnnotations(a1: Int)

  "AvroType.toSchema" should "preserve surrounding user defined annotations" in {
    containsAllAnnotTypes[RecordWithSurroundingAnnotations]
  }

  @AvroType.toSchema
  @Annotation1
  @Annotation2
  case class RecordWithSequentialAnnotations(a1: Int)

  it should "preserve sequential user defined annotations" in {
    containsAllAnnotTypes[RecordWithSequentialAnnotations]
  }

  def containsAllAnnotTypes[T: TypeTag]: Assertion =
    typeOf[T].typeSymbol.annotations
      .map(_.tree.tpe)
      .containsSlice(Seq(typeOf[Annotation1], typeOf[Annotation2])) shouldBe true

}
