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

import java.util.{List => JList}

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type._

import scala.collection.JavaConverters._

/** Utility for Avro schemas. */
object SchemaUtil {

  /** Convert schema to case class definitions. */
  def toPrettyString(schema: Schema, indent: Int = 0): String =
    getCaseClass(schema.getFields, schema.getName, indent)

  // scalastyle:off cyclomatic.complexity
  private def getFieldType(fieldName: String,
                           fieldSchema: Schema,
                           indent: Int): (String, Seq[String]) = {
    fieldSchema.getType match {
      case BOOLEAN => ("Boolean", Seq.empty)
      case INT => ("Int", Seq.empty)
      case LONG => ("Long", Seq.empty)
      case FLOAT => ("Float", Seq.empty)
      case DOUBLE => ("Double", Seq.empty)
      case STRING | ENUM => ("String", Seq.empty)
      case BYTES => ("ByteString", Seq.empty)
      case ARRAY =>
        val (fieldType, nested) = getFieldType(fieldName, fieldSchema.getElementType, indent)
        (s"List[$fieldType]", nested)
      case MAP =>
        val (fieldType, nested) = getFieldType(fieldName, fieldSchema.getValueType, indent)
        (s"Map[String, $fieldType]", nested)
      case UNION =>
        val unionTypes = fieldSchema.getTypes.asScala.map(_.getType).distinct
        if (unionTypes.size != 2 || !unionTypes.contains(NULL)) {
          throw new IllegalArgumentException(
            s"type: ${fieldSchema.getType} is not supported. " +
            s"Union type needs to contain exactly one 'null' type and one non null type.")
        }
        val (fieldType, nested) = getFieldType(fieldName, fieldSchema.getTypes.get(1), indent)
        (s"Option[$fieldType] = None", nested)
      case RECORD =>
        val className = NameProvider.getUniqueName(fieldSchema.getName)
        val nested = getCaseClass(fieldSchema.getFields, className, indent)
        (className, Seq(nested))
      case t => throw new IllegalArgumentException(s"Type: $t not supported")
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def getCaseClass(fields: JList[Field], name: String, indent: Int): String = {
    val xs = fields.asScala
      .map { f =>
        val (fieldType, nested) = getFieldType(f.name, f.schema, indent)
        (escapeNameIfReserved(f.name) + ": " + fieldType, nested)
      }
    val lines = xs.map(_._1)
    val nested = xs.flatMap(_._2)

    val sb = StringBuilder.newBuilder
    sb.append(s"case class $name(")
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
    (sb.toString() +: nested).mkString("\n")
  }

  private[types] def escapeNameIfReserved(name: String): String = {
    if (scalaReservedWords.contains(name)) {
      s"`$name`"
    } else {
      name
    }
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
    "abstract", "case", "catch", "class", "def", "do", "else", "extends", "false", "final",
    "finally", "for", "forSome", "if", "implicit", "import", "lazy", "match", "new", "null",
    "object", "override", "package", "private", "protected", "return", "sealed", "super", "this",
    "throw", "trait", "try", "true", "type", "val", "var", "while", "with", "yield")

}
