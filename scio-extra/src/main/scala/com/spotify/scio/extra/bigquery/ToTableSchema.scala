/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.extra.bigquery

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.extra.bigquery.Implicits.AvroConversionException
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type._

import scala.collection.JavaConverters._

/**
 * Converts a [[org.apache.avro.Schema Schema]] object into a
 * [[com.google.api.services.bigquery.model.TableSchema]] TableSchema.
 * All Avro primitive and complex types are supported.
 */
trait ToTableSchema {
  private lazy val avroToBQTypes: Map[Type, String] = Map(
    STRING -> "STRING",
    ENUM -> "STRING",
    BYTES -> "BYTES",
    INT -> "INTEGER",
    LONG -> "INTEGER",
    FLOAT -> "FLOAT",
    DOUBLE -> "FLOAT",
    BOOLEAN -> "BOOLEAN",
    RECORD -> "RECORD",
    FIXED -> "BYTES"
  )

  private lazy val supportedAvroTypes: Set[Type] =
    (avroToBQTypes.keys ++ Seq(UNION, ARRAY, RECORD, MAP)).toSet

  /**
   * Traverses all fields of the supplied avroSchema and converts it into
   * a TableSchema containing TableFieldSchemas.
   *
   * @param avroSchema
   * @return the equivalent BigQuery schema
   */
  def toTableSchema(avroSchema: Schema): TableSchema = {
    val fields = getFieldSchemas(avroSchema)

    new TableSchema().setFields(fields.asJava)
  }

  private def getFieldSchemas(avroSchema: Schema): List[TableFieldSchema] = {
    avroSchema.getFields.asScala.map { field =>
      val tableField = new TableFieldSchema()
        .setName(field.name)

      Option(field.doc).foreach(tableField.setDescription)

      setFieldType(tableField, field.schema)
      tableField
    }.toList
  }

  private def setFieldType(field: TableFieldSchema, schema: Schema): Unit = {
    val schemaType = schema.getType

    if (!supportedAvroTypes.contains(schemaType)) {
      throw AvroConversionException(s"Could not match type $schemaType")
    }

    if (schemaType != UNION && Option(field.getMode).isEmpty) {
      field.setMode("REQUIRED")
    }

    avroToBQTypes.get(schemaType).foreach { bqType =>
      field.setType(bqType)
    }

    schemaType match {
      case UNION  => setFieldDataTypeFromUnion(field, schema)
      case ARRAY  => setFieldDataTypeFromArray(field, schema)
      case RECORD => field.setFields(getFieldSchemas(schema).asJava)
      case MAP    => setFieldTypeFromMap(field, schema)
      case _      =>
    }
  }

  private def setFieldDataTypeFromUnion(field: TableFieldSchema, schema: Schema): Unit = {
    if (schema.getTypes.size != 2) {
      throw AvroConversionException("Union fields with > 2 types not supported")
    }

    if (Option(field.getMode).contains("REPEATED")) {
      throw AvroConversionException("Array of unions is not supported")
    }

    if (schema.getTypes.asScala.count(_.getType == NULL) != 1) {
      throw AvroConversionException("Union field must include null type")
    }

    field.setMode("NULLABLE")

    schema.getTypes.asScala
      .find(_.getType != NULL)
      .foreach { fieldType =>
        setFieldType(field, fieldType)
      }
  }

  private def setFieldDataTypeFromArray(field: TableFieldSchema, schema: Schema): Unit = {
    if (field.getMode.equals("REPEATED")) {
      throw AvroConversionException("Array of arrays not supported")
    }

    field.setMode("REPEATED")
    setFieldType(field, schema.getElementType)
  }

  private def setFieldTypeFromMap(field: TableFieldSchema, schema: Schema): Unit = {
    if (field.getMode.equals("REPEATED")) {
      throw AvroConversionException("Array of maps not supported")
    }

    field.setMode("REPEATED")
    field.setType("RECORD")

    val keyField = new TableFieldSchema()
      .setName("key")
      .setType("STRING")
      .setMode("REQUIRED")
    val valueField = new TableFieldSchema().setName("value")
    setFieldType(valueField, schema.getValueType)

    field.setFields(List(keyField, valueField).asJava)
  }
}
