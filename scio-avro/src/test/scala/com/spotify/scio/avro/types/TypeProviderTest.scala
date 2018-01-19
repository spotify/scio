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

import com.google.protobuf.ByteString
import com.spotify.scio.avro.types.AvroType.HasAvroDoc
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FlatSpec, Matchers}

class TypeProviderTest extends FlatSpec with Matchers {
  @AvroType.fromSchema(
    """{"type":"record","name": "Record","fields":[{"name":"f1","type":"int"}]}""")
  class StringLiteralRecord

  @AvroType.fromSchema(
    """
      {"type":"record","name": "Record","fields":[{"name":"f1","type":"int"}]}
    """)
  class MultiLineStringLiteral

  @AvroType.fromSchema(
    """
      |{"type":"record","name": "Record","fields":[{"name":"f1","type":"int"}]}
    """.stripMargin)
  class MultiLineStripMarginStringLiteral

  @AvroType.fromSchema(
    """
      |{"type":"record","name":"Record","fields":[{"name":"f1","type":"int"}]}
    """.stripMargin)
  @doc("Table Description")
  class TableWithDescription

  "AvroType.fromSchema" should "support string literal" in {
    val r = StringLiteralRecord(1)
    r.f1 shouldBe 1
  }

  it should "support multi-line string literal" in {
    val r = MultiLineStringLiteral(1)
    r.f1 shouldBe 1
  }

  it should "support multi-line string literal with stripMargin" in {
    val r = MultiLineStripMarginStringLiteral(1)
    r.f1 shouldBe 1
  }

  it should "support doc annotation" in {
    TableWithDescription.doc shouldBe "Table Description"
    TableWithDescription.isInstanceOf[HasAvroDoc] shouldBe true
  }

  @AvroType.fromSchema(
    """
       |{
       |  "type": "record",
       |  "name": "Record",
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
  class RecordWithBasicTypes

  it should "support required primitive types" in {
    val r = RecordWithBasicTypes(true, 1, 2L, 1.5f, 2.5, "string",
      ByteString.copyFromUtf8("bytes"))
    r.boolF shouldBe true
    r.intF shouldBe 1
    r.longF shouldBe 2L
    r.floatF shouldBe 1.5f
    r.doubleF shouldBe 2.5
    r.stringF shouldBe "string"
    r.byteStringF shouldBe ByteString.copyFromUtf8("bytes")
  }

  it should "support .tupled in companion object" in {
    val r1 = RecordWithBasicTypes(true, 1, 2L, 1.5f, 2.5, "string",
      ByteString.copyFromUtf8("bytes"))
    val r2 = RecordWithBasicTypes.tupled((true, 1, 2L, 1.5f, 2.5, "string",
      ByteString.copyFromUtf8("bytes")))
    r1 shouldBe r2
  }

  it should "return correct schema for plain records" in {
    RecordWithBasicTypes.schema shouldBe
      new Schema.Parser().parse("""
                                 |{
                                 |  "type": "record",
                                 |  "name": "Record",
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
    (classOf[(GenericRecord => RecordWithBasicTypes)]
      isAssignableFrom RecordWithBasicTypes.fromGenericRecord.getClass) shouldBe true
  }

  it should "support .toGenericRecord in companion object" in {
    (classOf[(RecordWithBasicTypes => GenericRecord)]
      isAssignableFrom RecordWithBasicTypes.toGenericRecord.getClass) shouldBe true
  }

  it should "support round trip conversion from GenericRecord" in {
    val r1 = RecordWithBasicTypes(true, 1, 2L, 1.5f, 2.5, "string",
      ByteString.copyFromUtf8("bytes"))
    val r2 = RecordWithBasicTypes.fromGenericRecord(RecordWithBasicTypes.toGenericRecord(r1))
    r1 shouldBe r2
  }

  @AvroType.fromSchema(
    """
      |{
      |  "type": "record",
      |  "name": "Record",
      |  "fields": [
      |    { "name": "boolF", "type": ["null", "boolean"], "default": null},
      |    { "name": "intF", "type": ["null", "int"], "default": null},
      |    { "name": "longF", "type": ["null", "long"], "default": null},
      |    { "name": "floatF", "type": ["null", "float"], "default": null},
      |    { "name": "doubleF", "type": ["null", "double"], "default": null},
      |    { "name": "stringF", "type": ["null", "string"], "default": null},
      |    { "name": "byteStringF", "type": ["null", "bytes"], "default": null}
      |  ]
      |}
    """.stripMargin)
  class RecordWithOptionalBasicTypes

  it should "support nullable primitive types" in {
    val r = RecordWithOptionalBasicTypes(
      Some(true), Some(1), Some(2L), Some(1.5f),
      Some(2.5), Some("string"), Some(ByteString.copyFromUtf8("bytes")))
    r.boolF shouldBe Some(true)
    r.intF shouldBe Some(1)
    r.longF shouldBe Some(2L)
    r.floatF shouldBe Some(1.5f)
    r.doubleF shouldBe Some(2.5)
    r.stringF shouldBe Some("string")
    r.byteStringF shouldBe Some(ByteString.copyFromUtf8("bytes"))
  }

  it should "set nullable values to None by default" in {
    val r = RecordWithOptionalBasicTypes()
    r.boolF shouldBe None
    r.intF shouldBe None
    r.longF shouldBe None
    r.floatF shouldBe None
    r.doubleF shouldBe None
    r.stringF shouldBe None
    r.byteStringF shouldBe None
  }

  @AvroType.fromSchema(
    """
      |{
      |  "type": "record",
      |  "name": "Record",
      |  "fields": [
      |    { "name": "boolF", "type": { "type" : "array", "items" : "boolean"}},
      |    { "name": "intF", "type": { "type" : "array", "items" : "int"}},
      |    { "name": "longF", "type": { "type" : "array", "items" : "long"}},
      |    { "name": "floatF", "type": { "type" : "array", "items" : "float"}},
      |    { "name": "doubleF", "type": { "type" : "array", "items" : "double"}},
      |    { "name": "stringF", "type": { "type" : "array", "items" : "string"}},
      |    { "name": "byteStringF", "type": { "type" : "array", "items" : "bytes"}}
      |  ]
      |}
    """.stripMargin)
  class RecordWithBasicTypeArrays

  it should "support primitive type arrays" in {
    val r = RecordWithBasicTypeArrays(
      List(true), List(1), List(2L), List(1.5f),
      List(2.5), List("string"), List(ByteString.copyFromUtf8("bytes")))
    r.boolF shouldBe List(true)
    r.intF shouldBe List(1)
    r.longF shouldBe List(2L)
    r.floatF shouldBe List(1.5f)
    r.doubleF shouldBe List(2.5)
    r.stringF shouldBe List("string")
    r.byteStringF shouldBe List(ByteString.copyFromUtf8("bytes"))
  }

  @AvroType.fromSchema(
    """
      |{
      |  "type": "record",
      |  "name": "Record",
      |  "fields": [
      |    { "name": "boolF", "type": { "type" : "map", "values" : "boolean"}},
      |    { "name": "intF", "type": { "type" : "map", "values" : "int"}},
      |    { "name": "longF", "type": { "type" : "map", "values" : "long"}},
      |    { "name": "floatF", "type": { "type" : "map", "values" : "float"}},
      |    { "name": "doubleF", "type": { "type" : "map", "values" : "double"}},
      |    { "name": "stringF", "type": { "type" : "map", "values" : "string"}},
      |    { "name": "byteStringF", "type": { "type" : "map", "values" : "bytes"}}
      |  ]
      |}
    """.stripMargin)
  class RecordWithBasicTypeMaps

  it should "support primitive type maps" in {
    val r = RecordWithBasicTypeMaps(
      Map("bool" -> true), Map("int" -> 1),
      Map("long" -> 2L), Map("float" -> 1.5f),
      Map("double" -> 2.5), Map("string" -> "string"),
      Map("bytes" -> ByteString.copyFromUtf8("bytes")))
    r.boolF shouldBe Map("bool" -> true)
    r.intF shouldBe Map("int" -> 1)
    r.longF shouldBe Map("long" -> 2L)
    r.floatF shouldBe Map("float" -> 1.5f)
    r.doubleF shouldBe Map("double" -> 2.5)
    r.stringF shouldBe Map("string" -> "string")
    r.byteStringF shouldBe Map("bytes" -> ByteString.copyFromUtf8("bytes"))
  }

  @AvroType.fromSchema(
    """
      |{
      |  "type" : "record",
      |  "name" : "Record",
      |  "fields" : [
      |    { "name" : "basic",
      |      "type" : {
      |        "type": "record",
      |        "name": "Basic",
      |        "fields": [
      |          { "name": "intF", "type": "int"}
      |        ]}
      |    },
      |    { "name" : "optional",
      |      "type" : {
      |        "type": "record",
      |        "name": "Optional",
      |        "fields": [
      |          { "name": "intF", "type": ["null","int"], "default": null}
      |        ]}
      |    },
      |    { "name" : "array",
      |      "type" : {
      |        "type": "record",
      |        "name": "Array",
      |        "fields": [
      |          { "name": "intF", "type": { "type" : "array", "items" : "int"}}
      |        ]}
      |    },
      |    { "name" : "map",
      |      "type" : {
      |        "type": "record",
      |        "name": "Map",
      |        "fields": [
      |          { "name": "intF", "type": { "type" : "map", "values" : "int"}}
      |        ]}
      |    }
      |  ]
      |}
      |""".stripMargin)
  class RecordWithRecords

  it should "support nested records" in {
    val r = RecordWithRecords(RecordWithRecords$Basic(1), RecordWithRecords$Optional(Some(1)),
      RecordWithRecords$Array(List(1)), RecordWithRecords$Map(Map("int" -> 1)))
    r.basic.intF shouldBe 1
    r.optional.intF shouldBe Some(1)
    r.array.intF shouldBe List(1)
    r.map.intF shouldBe Map("int" -> 1)
  }

  it should "return correct schema for nested objects" in {
    RecordWithRecords.schema shouldBe
      new Schema.Parser().parse("""
                                  |{
                                  |  "type" : "record",
                                  |  "name" : "Record",
                                  |  "fields" : [
                                  |    { "name" : "basic",
                                  |      "type" : {
                                  |        "type": "record",
                                  |        "name": "Basic",
                                  |        "fields": [
                                  |          { "name": "intF", "type": "int"}
                                  |        ]}
                                  |    },
                                  |    { "name" : "optional",
                                  |      "type" : {
                                  |        "type": "record",
                                  |        "name": "Optional",
                                  |        "fields": [
                                  |          { "name": "intF",
                                  |            "type": ["null","int"],
                                  |            "default": null}
                                  |        ]}
                                  |    },
                                  |    { "name" : "array",
                                  |      "type" : {
                                  |        "type": "record",
                                  |        "name": "Array",
                                  |        "fields": [
                                  |          { "name": "intF",
                                  |            "type": { "type" : "array", "items" : "int"}}
                                  |        ]}
                                  |    },
                                  |    { "name" : "map",
                                  |      "type" : {
                                  |        "type": "record",
                                  |        "name": "Map",
                                  |        "fields": [
                                  |          { "name": "intF",
                                  |            "type": { "type" : "map", "values" : "int"}}
                                  |        ]}
                                  |    }
                                  |  ]
                                  |}
                                  |""".stripMargin)
  }

  @AvroType.fromSchema(
    """
      |{
      |  "type" : "record",
      |  "name" : "Record",
      |  "fields" : [
      |    { "name" : "basic",
      |      "type" : ["null", {
      |        "type": "record",
      |        "name": "Basic",
      |        "fields": [
      |          { "name": "intF", "type": "int"}
      |        ]}]
      |    },
      |    { "name" : "optional",
      |      "type" : ["null", {
      |        "type": "record",
      |        "name": "Optional",
      |        "fields": [
      |          { "name": "intF", "type": ["null","int"], "default": null}
      |        ]}]
      |    },
      |    { "name" : "array",
      |      "type" : ["null", {
      |        "type": "record",
      |        "name": "Array",
      |        "fields": [
      |          { "name": "intF", "type": { "type" : "array", "items" : "int"}}
      |        ]}]
      |    },
      |    { "name" : "map",
      |      "type" : ["null", {
      |        "type": "record",
      |        "name": "Map",
      |        "fields": [
      |          { "name": "intF", "type": { "type" : "map", "values" : "int"}}
      |        ]}]
      |    }
      |  ]
      |}
      |""".stripMargin)
  class RecordWithOptionalRecords

  it should "support optional nested records" in {
    val r = RecordWithOptionalRecords(
      Some(RecordWithOptionalRecords$Basic(1)),
      Some(RecordWithOptionalRecords$Optional(Some(1))),
      Some(RecordWithOptionalRecords$Array(List(1))),
      Some(RecordWithOptionalRecords$Map(Map("int" -> 1))))
    r.basic shouldBe Some(RecordWithOptionalRecords$Basic(1))
    r.optional shouldBe Some(RecordWithOptionalRecords$Optional(Some(1)))
    r.array shouldBe Some(RecordWithOptionalRecords$Array(List(1)))
    r.map shouldBe Some(RecordWithOptionalRecords$Map(Map("int" -> 1)))
  }

  @AvroType.fromSchema(
    """
      |{
      |  "type" : "record",
      |  "name" : "Record",
      |  "fields" : [
      |    { "name" : "basic",
      |      "type" : { "type" : "array", "items" : {
      |        "type": "record",
      |        "name": "Basic",
      |        "fields": [
      |          { "name": "intF", "type": "int"}
      |        ]}}
      |    },
      |    { "name" : "optional",
      |      "type" : { "type" : "array", "items" : {
      |        "type": "record",
      |        "name": "Optional",
      |        "fields": [
      |          { "name": "intF", "type": ["null","int"], "default": null}
      |        ]}}
      |    },
      |    { "name" : "array",
      |      "type" : { "type" : "array", "items" : {
      |        "type": "record",
      |        "name": "Array",
      |        "fields": [
      |          { "name": "intF", "type": { "type" : "array", "items" : "int"}}
      |        ]}}
      |    },
      |    { "name" : "map",
      |      "type" : { "type" : "array", "items" : {
      |        "type": "record",
      |        "name": "Map",
      |        "fields": [
      |          { "name": "intF", "type": { "type" : "map", "values" : "int"}}
      |        ]}}
      |    }
      |  ]
      |}
      |""".stripMargin)
  class RecordWithRecordArrays

  it should "support nested record arrays" in {
    val r = RecordWithRecordArrays(
      List(RecordWithRecordArrays$Basic(1)),
      List(RecordWithRecordArrays$Optional(Some(1))),
      List(RecordWithRecordArrays$Array(List(1))),
      List(RecordWithRecordArrays$Map(Map("int" -> 1))))
    r.basic shouldBe List(RecordWithRecordArrays$Basic(1))
    r.optional shouldBe List(RecordWithRecordArrays$Optional(Some(1)))
    r.array shouldBe List(RecordWithRecordArrays$Array(List(1)))
    r.map shouldBe List(RecordWithRecordArrays$Map(Map("int" -> 1)))
  }

  @AvroType.fromSchema(
    """
      |{
      |  "type" : "record",
      |  "name" : "Record",
      |  "fields" : [
      |    { "name" : "basic",
      |      "type" : { "type" : "map", "values" : {
      |        "type": "record",
      |        "name": "Basic",
      |        "fields": [
      |          { "name": "intF", "type": "int"}
      |        ]}}
      |    },
      |    { "name" : "optional",
      |      "type" : { "type" : "map", "values" : {
      |        "type": "record",
      |        "name": "Optional",
      |        "fields": [
      |          { "name": "intF", "type": ["null","int"], "default": null}
      |        ]}}
      |    },
      |    { "name" : "array",
      |      "type" : { "type" : "map", "values" : {
      |        "type": "record",
      |        "name": "Array",
      |        "fields": [
      |          { "name": "intF", "type": { "type" : "array", "items" : "int"}}
      |        ]}}
      |    },
      |    { "name" : "map",
      |      "type" : { "type" : "map", "values" : {
      |        "type": "record",
      |        "name": "Map",
      |        "fields": [
      |          { "name": "intF", "type": { "type" : "map", "values" : "int"}}
      |        ]}}
      |    }
      |  ]
      |}
      |""".stripMargin)
  class RecordWithRecordMaps

  it should "support nested record maps" in {
    val r = RecordWithRecordMaps(
      Map("basic" -> RecordWithRecordMaps$Basic(1)),
      Map("optional" -> RecordWithRecordMaps$Optional(Some(1))),
      Map("array" -> RecordWithRecordMaps$Array(List(1))),
      Map("map" -> RecordWithRecordMaps$Map(Map("int" -> 1))))
    r.basic shouldBe Map("basic" -> RecordWithRecordMaps$Basic(1))
    r.optional shouldBe Map("optional" -> RecordWithRecordMaps$Optional(Some(1)))
    r.array shouldBe  Map("array" -> RecordWithRecordMaps$Array(List(1)))
    r.map shouldBe Map("map" -> RecordWithRecordMaps$Map(Map("int" -> 1)))
  }

  @AvroType.fromSchema(
    """
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
  class RecordWithNestedRecords

  it should "support multiple levels of nesting records" in {
    val r =
      RecordWithNestedRecords(
        RecordWithNestedRecords$Level1(
          RecordWithNestedRecords$Level1$Level2(
            RecordWithNestedRecords$Level1$Level2$Level3(1))))
    r.level1.level2.level3.intF shouldBe 1
  }

  @AvroType.fromSchema(
    """
      |{"type":"record","name": "Record","fields":[{"name":"f1","type":"int"}]}
    """.stripMargin)
  class Artisanal1Field

  it should "not provide .tupled in companion object with single field" in {
    Artisanal1Field.getClass.getMethods.map(_.getName) should not contain "tupled"
  }


  @AvroType.toSchema
  case class ToSchema(boolF: Boolean,
                     intF: Int,
                     longF: Long,
                     floatF: Float,
                     doubleF: Double,
                     stringF: String,
                     bytesF: ByteString)

  "AvroType.toSchema" should "support .tupled in companion object" in {
    val r1 = ToSchema(true, 1, 2L, 1.5f, 2.5, "string", ByteString.copyFromUtf8("bytes"))
    val r2 = ToSchema.tupled((true, 1, 2L, 1.5f, 2.5, "string", ByteString.copyFromUtf8("bytes")))
    r1 shouldBe r2
  }

  it should "returns correct schema for plain objects" in {
    ToSchema.schema should be
      new Schema.Parser().parse("""
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
    (classOf[(GenericRecord => ToSchema)] isAssignableFrom
      ToSchema.fromGenericRecord.getClass) shouldBe true
  }

  it should "support .toGenericRecord in companion object" in {
    (classOf[(ToSchema => GenericRecord)] isAssignableFrom
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
    def doApply(f: (Boolean, Int, Long, Float, Double, String, ByteString) => ToSchema)
               (x: (Boolean, Int, Long, Float, Double, String, ByteString)): ToSchema =
      f(x._1, x._2, x._3, x._4, x._5, x._6, x._7)

    doApply(ToSchema.apply _)(
      (true, 1, 2L, 1.5f, 2.5, "string", ByteString.copyFromUtf8("bytes"))) shouldBe
      doApply(ToSchema)((true, 1, 2L, 1.5f, 2.5, "string", ByteString.copyFromUtf8("bytes")))

    doApply(ToSchema)(
      (true, 1, 2L, 1.5f, 2.5, "string", ByteString.copyFromUtf8("bytes"))) shouldBe
      ToSchema(true, 1, 2L, 1.5f, 2.5, "string", ByteString.copyFromUtf8("bytes"))
  }

  @AvroType.toSchema
  case class ReservedName(`type`: Boolean,
                          `int`: Int)

  it should "returns correct schema for objects containing fields named as scala reserved word" in {
    ReservedName.schema should be
    new Schema.Parser().parse("""
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
    Artisanal1FieldToSchema.getClass.getMethods.map(_.getName) should not contain "tupled"
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

  @AvroType.fromSchema(
    """
      |{
      |  "type": "record",
      |  "name": "Record",
      |  "fields": [
      |    { "name": "f1", "type": "int"},
      |    { "name": "f2", "type": "int"},
      |    { "name": "f3", "type": "int"},
      |    { "name": "f4", "type": "int"},
      |    { "name": "f5", "type": "int"},
      |    { "name": "f6", "type": "int"},
      |    { "name": "f7", "type": "int"},
      |    { "name": "f8", "type": "int"},
      |    { "name": "f9", "type": "int"},
      |    { "name": "f10", "type": "int"},
      |    { "name": "f11", "type": "int"},
      |    { "name": "f12", "type": "int"},
      |    { "name": "f13", "type": "int"},
      |    { "name": "f14", "type": "int"},
      |    { "name": "f15", "type": "int"},
      |    { "name": "f16", "type": "int"},
      |    { "name": "f17", "type": "int"},
      |    { "name": "f18", "type": "int"},
      |    { "name": "f19", "type": "int"},
      |    { "name": "f20", "type": "int"},
      |    { "name": "f21", "type": "int"},
      |    { "name": "f22", "type": "int"},
      |    { "name": "f23", "type": "int"}
      |  ]
      |}
    """.stripMargin)
  class ArtisanalMoreThan22Fields

  "AvroType.fromSchema" should "support .schema in companion object with >22 fields" in {
    ArtisanalMoreThan22Fields(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
    ArtisanalMoreThan22Fields.schema should not be null
  }

  it should "not provide .tupled in companion object with >22 fields" in {
    ArtisanalMoreThan22Fields.getClass.getMethods.map(_.getName) should not contain "tupled"
  }

  @AvroType.toSchema
  case class TwentyThree(a1:Int,a2:Int,a3:Int,a4:Int,a5:Int,a6:Int,a7:Int,a8:Int,a9:Int,a10:Int,
                         a11:Int,a12:Int,a13:Int,a14:Int,a15:Int,a16:Int,a17:Int,a18:Int,a19:Int,
                         a20:Int,a21:Int,a22:Int,a23:Int)

  "AvroType.toSchema" should "not provide .tupled in companion object with >22 fields" in {
    TwentyThree.getClass.getMethods.map(_.getName) should not contain "tupled"
  }

  it should "support .schema in companion object with >22 fields" in {
    TwentyThree.schema should not be null
  }

  it should "support .fromGenericRecord in companion object with >22 fields" in {
    val cls = classOf[(GenericRecord => TwentyThree)]
    (cls isAssignableFrom TwentyThree.fromGenericRecord.getClass) shouldBe true
  }

  it should "support .toGenericRecord in companion object with >22 fields" in {
    val cls = classOf[(TwentyThree => GenericRecord)]
    (cls isAssignableFrom TwentyThree.toGenericRecord.getClass) shouldBe true
  }

  @AvroType.toSchema
  @doc("Avro doc")
  case class DocumentedRecord(a1:Int)

  it should "support table description" in {
    DocumentedRecord.doc shouldBe "Avro doc"
    DocumentedRecord.isInstanceOf[HasAvroDoc] shouldBe true
  }

  @AvroType.fromSchemaFile(
    """
      |scio-avro/src/test/avro/
      |scio-avro-test.avsc
    """.stripMargin)
  class FromResourceMultiLine

  @AvroType.fromSchemaFile("scio-avro/src/test/avro/scio-avro-test.avsc")
  class FromResource

  "AvroType.fromSchemaFile" should "support reading schema from multiline resource" in {
    val r = FromResourceMultiLine(1)
    r.test shouldBe 1
  }

  it should "support reading schema from resource" in {
    val r = FromResource(2)
    r.test shouldBe 2
  }

  @AvroType.fromSchema(
    """
      |{
      |  "name": "A", "namespace": "outer",
      |  "type": "record", "fields": [
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "record",
      |        "name": "A", "namespace": "inner",
      |        "fields": [{"name": "intF", "type": "int"}]
      |      }
      |    }
      |  ]
      |}
    """.stripMargin)
  class SameName

  it should "support nested record with same name as enclosing record" in {
    val r = SameName(SameName$A(2))
    SameName.toGenericRecord(r).getSchema.toString
  }
}
