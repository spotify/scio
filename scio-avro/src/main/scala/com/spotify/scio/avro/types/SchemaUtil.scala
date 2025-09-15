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

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._

import scala.jdk.CollectionConverters._

/** Utility for Avro schemas. */
object SchemaUtil {

  /** Convert schema to case class definitions. */
  def toPrettyString1(schema: Schema, indent: Int = 0): String =
    toPrettyString(schema.getName, schema, indent)

  /** Convert schema to case class definitions. */
  def toPrettyString(className: String, schema: Schema, indent: Int): String =
    getCaseClass(className, schema, indent)

  private def getFieldType(
    className: String,
    fieldName: String,
    fieldSchema: Schema,
    indent: Int
  ): (String, Seq[String]) =
    fieldSchema.getType match {
      case BOOLEAN       => ("Boolean", Seq.empty)
      case INT           => ("Int", Seq.empty)
      case LONG          => ("Long", Seq.empty)
      case FLOAT         => ("Float", Seq.empty)
      case DOUBLE        => ("Double", Seq.empty)
      case STRING | ENUM => ("String", Seq.empty)
      case BYTES         => ("ByteString", Seq.empty)
      case ARRAY         =>
        val (fieldType, nested) =
          getFieldType(className, fieldName, fieldSchema.getElementType, indent)
        (s"List[$fieldType]", nested)
      case MAP =>
        val (fieldType, nested) =
          getFieldType(className, fieldName, fieldSchema.getValueType, indent)
        (s"Map[String,$fieldType]", nested)
      case UNION =>
        val unionTypes = fieldSchema.getTypes.asScala.map(_.getType).distinct
        if (unionTypes.size != 2 || !unionTypes.contains(NULL)) {
          throw new IllegalArgumentException(
            s"type: ${fieldSchema.getType} is not supported. " +
              s"Union type needs to contain exactly one 'null' type and one non null type."
          )
        }
        val (fieldType, nested) =
          getFieldType(
            className,
            fieldName,
            fieldSchema.getTypes.asScala.filter(_.getType != NULL).head,
            indent
          )
        (s"Option[$fieldType] = None", nested)
      case RECORD =>
        val nestedClassName = s"$className$$${fieldSchema.getName}"
        val nested =
          getCaseClass(nestedClassName, fieldSchema, indent)
        (nestedClassName, Seq(nested))
      case t => throw new IllegalArgumentException(s"Type: $t not supported")
    }

  private def getCaseClass(className: String, schema: Schema, indent: Int): String = {
    val xs = schema.getFields.asScala
      .map { f =>
        val (fieldType, nested) =
          getFieldType(className, f.name, f.schema, indent)
        (escapeNameIfReserved(f.name) + ": " + fieldType, nested)
      }
    val lines = xs.map(_._1)
    val nested = xs.flatMap(_._2)

    val sb = new StringBuilder
    sb.append(s"case class $className(")
    if (indent > 0) {
      sb.append("\n")
    }
    val body = if (indent > 0) {
      val w = " " * indent
      lines.map(w + _).mkString(",\n")
    } else {
      lines.mkString(", ")
    }
    sb.append(body)
    sb.append(")")
    (sb.toString +: nested).mkString("\n")
  }

  private[types] def escapeNameIfReserved(name: String): String =
    if (scalaReservedWords.contains(name)) {
      s"`$name`"
    } else {
      name
    }

  private[types] def unescapeNameIfReserved(name: String): String = {
    val Pattern = "^`(.*)`$".r
    name match {
      case Pattern(unescapedName) =>
        if (scalaReservedWords.contains(unescapedName)) {
          unescapedName
        } else {
          name
        }
      case _ => name
    }
  }

  private[types] val scalaReservedWords = Seq(
    "abstract",
    "case",
    "catch",
    "class",
    "def",
    "do",
    "else",
    "extends",
    "false",
    "final",
    "finally",
    "for",
    "forSome",
    "if",
    "implicit",
    "import",
    "lazy",
    "match",
    "new",
    "null",
    "object",
    "override",
    "package",
    "private",
    "protected",
    "return",
    "sealed",
    "super",
    "this",
    "throw",
    "trait",
    "try",
    "true",
    "type",
    "val",
    "var",
    "while",
    "with",
    "yield"
  )
}
