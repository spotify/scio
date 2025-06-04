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

package com.spotify.scio.bigquery

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import org.apache.avro.Schema
import org.apache.avro.Schema.Type

import scala.jdk.CollectionConverters._

/** Utility for BigQuery Storage API. */
object StorageUtil {
  def tableReadOptions(
    selectedFields: List[String] = Nil,
    rowRestriction: Option[String] = None
  ): TableReadOptions =
    TableReadOptions
      .newBuilder()
      .addAllSelectedFields(selectedFields.asJava)
      .setRowRestriction(rowRestriction.getOrElse(""))
      .build()

  // https://cloud.google.com/bigquery/docs/reference/storage/
  def toTableSchema(avroSchema: Schema): TableSchema = {
    val fields = getFieldSchemas(avroSchema)

    new TableSchema().setFields(fields.asJava)
  }

  private def getFieldSchemas(avroSchema: Schema): List[TableFieldSchema] =
    avroSchema.getFields.asScala.map(toTableFieldSchema).toList

  private def toTableFieldSchema(field: Schema.Field): TableFieldSchema = {
    val schema = field.schema
    val (mode, tpe) = schema.getType match {
      case Type.UNION =>
        val types = schema.getTypes
        assert(types.size == 2 && types.get(0).getType == Type.NULL)
        ("NULLABLE", types.get(1))
      case Type.ARRAY =>
        ("REPEATED", schema.getElementType)
      case _ =>
        ("REQUIRED", schema)
    }
    val tableField = new TableFieldSchema().setName(field.name).setMode(mode)
    setRawType(tableField, tpe)
    tableField
  }

  private def setRawType(tableField: TableFieldSchema, schema: Schema): Unit = {
    val tpe = schema.getType match {
      case Type.BOOLEAN => "BOOLEAN"
      case Type.LONG    =>
        schema.getLogicalType match {
          case null                                 => "INT64"
          case t if t.getName == "timestamp-micros" => "TIMESTAMP"
          case t if t.getName == "time-micros"      => "TIME"
          case t                                    =>
            throw new IllegalStateException(s"Unsupported logical type: $t")
        }
      case Type.DOUBLE => "FLOAT64"
      case Type.BYTES  =>
        schema.getLogicalType match {
          case null                        => "BYTES"
          case t if t.getName == "decimal" =>
            val precision = schema.getObjectProp("precision").asInstanceOf[Int]
            val scale = schema.getObjectProp("scale").asInstanceOf[Int]
            (precision, scale) match {
              case (38, 9)  => "NUMERIC"
              case (77, 38) => "BIGNUMERIC"
              case _        =>
                throw new IllegalStateException(
                  s"Unsupported decimal precision and scale: ($precision, $scale)"
                )
            }
          case t =>
            throw new IllegalStateException(s"Unsupported logical type: $t")
        }
      case Type.INT =>
        schema.getLogicalType match {
          case t if t.getName == "date" => "DATE"
          case t                        => s"Unsupported logical type: $t"
        }
      case Type.STRING =>
        // FIXME: schema.getLogicalType == null in this case, BigQuery service side bug?
        val logicalType = schema.getProp("logicalType")
        val sqlType = schema.getProp("sqlType")
        (logicalType, sqlType) match {
          case ("datetime", _)  => "DATETIME"
          case (_, "GEOGRAPHY") => "GEOGRAPHY"
          case (_, "JSON")      => "JSON"
          case _                => "STRING"
        }
      case Type.RECORD =>
        tableField.setFields(getFieldSchemas(schema).asJava)
        "RECORD"
      case t =>
        throw new IllegalStateException(s"Unsupported type: $t")
    }
    tableField.setType(tpe)
    ()
  }
}
