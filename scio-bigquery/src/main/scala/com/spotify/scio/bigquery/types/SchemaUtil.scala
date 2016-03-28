/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.bigquery.types

import java.util.{List => JList}

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}

import scala.collection.JavaConverters._

private object SchemaUtil {

  def toPrettyString(schema: TableSchema, indent: Int = 0): String = {
    getFields(schema.getFields, indent, 0, "(", ")")
  }

  private def getRawType(tfs: TableFieldSchema,
                         indent: Int = 0, level: Int = 0,
                         before: String = "", after: String = ""): String = tfs.getType match {
    case "INTEGER" => "Int"
    case "FLOAT" => "Double"
    case "BOOLEAN" => "Boolean"
    case "STRING" => "String"
    case "TIMESTAMP" => "Instant"
    case "RECORD" =>
      getFields(tfs.getFields, indent, level + 1, before + "(", ")" + after)
    case t => throw new IllegalArgumentException(s"Type: $t not supported")
  }

  private def getFieldType(tfs: TableFieldSchema,
                           indent: Int = 0, level: Int = 0,
                           before: String = "", after: String = ""): String = {
    val t = getRawType(tfs, indent, level)
    tfs.getMode match {
      case "NULLABLE" | null => getRawType(tfs, indent, level, "Option[", "]")
      case "REQUIRED" => t
      case "REPEATED" => getRawType(tfs, indent, level, "List[", "]")
    }
  }

  private def getFields(fields: JList[TableFieldSchema],
                        indent: Int = 0, level: Int = 0,
                        before: String = "", after: String = ""): String = {
    val lines = fields.asScala
      .map(f => f.getName + ": " + getFieldType(f, indent, level, before, after))
    indentLines(lines, indent, level + 1, before, after)
  }

  private def indentLines(lines: Seq[String],
                          indent: Int = 0, level: Int = 0,
                          before: String = "", after: String = ""): String = {
    val sb = StringBuilder.newBuilder
    if (before.nonEmpty) {
      sb.append(before)
      if (indent > 0) {
        sb.append("\n")
      }
    }
    val body = if (indent > 0) {
      val w = " " * indent * level
      lines.map(w + _).mkString(",\n")
    } else {
      lines.mkString(", ")
    }
    sb.append(body)
    if (after.nonEmpty) {
      sb.append(after)
    }
    sb.toString()
  }

}
