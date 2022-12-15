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
import org.apache.avro.Schema

object Schemas {
  case class BasicFields(
    boolF: Boolean,
    intF: Int,
    longF: Long,
    floatF: Float,
    doubleF: Double,
    stringF: String,
    byteStringF: ByteString
  )

  case class OptionalFields(
    boolF: Option[Boolean],
    intF: Option[Int],
    longF: Option[Long],
    floatF: Option[Float],
    doubleF: Option[Double],
    stringF: Option[String],
    byteStringF: Option[ByteString]
  )

  case class ArrayFields(
    boolF: List[Boolean],
    intF: List[Int],
    longF: List[Long],
    floatF: List[Float],
    doubleF: List[Double],
    stringF: List[String],
    byteStringF: List[ByteString]
  )

  case class MapFields(
    boolF: Map[String, Boolean],
    intF: Map[String, Int],
    longF: Map[String, Long],
    floatF: Map[String, Float],
    doubleF: Map[String, Double],
    stringF: Map[String, String],
    byteStringF: Map[String, ByteString]
  )

  case class NestedFields(
    basic: BasicFields,
    optional: OptionalFields,
    array: ArrayFields,
    map: MapFields
  )

  case class OptionalNestedFields(
    basic: Option[BasicFields],
    optional: Option[OptionalFields],
    array: Option[ArrayFields],
    map: Option[MapFields]
  )

  case class ArrayNestedFields(
    basic: List[BasicFields],
    optional: List[OptionalFields],
    array: List[ArrayFields],
    map: List[MapFields]
  )

  case class MapNestedFields(
    basic: Map[String, BasicFields],
    optional: Map[String, OptionalFields],
    array: Map[String, ArrayFields],
    map: Map[String, MapFields]
  )

  case class ByteArrayFields(
    required: Array[Byte],
    optional: Option[Array[Byte]],
    repeated: List[Array[Byte]]
  )

  object FieldMode extends Enumeration {
    type FieldMode = Value
    val ARRAY, MAP, OPTIONAL, NONE = Value
  }
  import FieldMode._

  def parseSchema(str: String): Schema = new Schema.Parser().parse(str)

  def basicFields(mode: FieldMode = NONE): String =
    s"""
       |{
       |  "type": "record",
       |  "namespace": "com.spotify.scio.avro.types.Schemas",
       |  "name": "${basicRecordName(mode)}",
       |  "fields": [
       |    { "name": "boolF",
       |      "type": ${fieldType("\"boolean\"", mode)}
       |      ${setDefaultValue(mode)}},
       |    { "name": "intF",
       |      "type": ${fieldType("\"int\"", mode)}
       |      ${setDefaultValue(mode)}},
       |    { "name": "longF",
       |      "type": ${fieldType("\"long\"", mode)}
       |      ${setDefaultValue(mode)}},
       |    { "name": "floatF",
       |      "type": ${fieldType("\"float\"", mode)}
       |      ${setDefaultValue(mode)}},
       |    { "name": "doubleF",
       |      "type": ${fieldType("\"double\"", mode)}
       |      ${setDefaultValue(mode)}},
       |    { "name": "stringF",
       |      "type": ${fieldType("\"string\"", mode)}
       |      ${setDefaultValue(mode)}},
       |    { "name": "byteStringF",
       |      "type": ${fieldType("\"bytes\"", mode)}
       |      ${setDefaultValue(mode)}}
       |  ]
       |}
       |""".stripMargin

  def recordFields(mode: FieldMode = NONE): String =
    s"""
       |{
       |  "type" : "record",
       |  "namespace": "com.spotify.scio.avro.types.Schemas",
       |  "name" : "${nestedRecordName(mode)}",
       |  "fields" : [
       |    { "name" : "basic",
       |      "type" : ${fieldType(basicFields(NONE), mode)}
       |      ${setDefaultValue(mode)}
       |    },
       |    { "name" : "optional",
       |      "type" : ${fieldType(basicFields(OPTIONAL), mode)}
       |      ${setDefaultValue(mode)}
       |    },
       |    { "name" : "array",
       |      "type" : ${fieldType(basicFields(ARRAY), mode)}
       |      ${setDefaultValue(mode)}
       |    },
       |    { "name" : "map",
       |      "type" : ${fieldType(basicFields(MAP), mode)}
       |      ${setDefaultValue(mode)}
       |    }
       |  ]
       |}
       |""".stripMargin

  private def fieldType(basicType: String, mode: FieldMode): String =
    mode match {
      case NONE     => basicType
      case OPTIONAL => s"""["null", $basicType]"""
      case ARRAY    => s"""{ "type" : "array", "items" : $basicType }"""
      case MAP      => s"""{ "type" : "map", "values" : $basicType }"""
      case _        => throw new IllegalArgumentException(s"Unsupported mode $mode")
    }

  private def setDefaultValue(mode: FieldMode): String =
    mode match {
      case NONE     => ""
      case OPTIONAL => s""", "default": null"""
      case ARRAY    => s""", "default": []"""
      case MAP      => s""", "default": {}"""
      case _        => throw new IllegalArgumentException(s"Unsupported mode $mode")
    }

  private def basicRecordName(mode: FieldMode): String =
    mode match {
      case NONE     => "BasicFields"
      case OPTIONAL => "OptionalFields"
      case ARRAY    => "ArrayFields"
      case MAP      => "MapFields"
      case _        => throw new IllegalArgumentException(s"Unsupported mode $mode")
    }

  private def nestedRecordName(mode: FieldMode): String =
    mode match {
      case NONE     => "NestedFields"
      case OPTIONAL => "OptionalNestedFields"
      case ARRAY    => "ArrayNestedFields"
      case MAP      => "MapNestedFields"
      case _        => throw new IllegalArgumentException(s"Unsupported mode $mode")
    }
}
