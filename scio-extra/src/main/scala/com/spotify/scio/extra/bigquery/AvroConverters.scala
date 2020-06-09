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

package com.spotify.scio.extra.bigquery

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.annotations.experimental
import com.spotify.scio.bigquery.TableRow
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

import scala.jdk.CollectionConverters._

object AvroConverters extends ToTableRow with ToTableSchema {

  @experimental
  def toTableRow[T <: IndexedRecord](record: T): TableRow = {
    val row = new TableRow

    record.getSchema.getFields.asScala.foreach { field =>
      Option(record.get(field.pos)).foreach { fieldValue =>
        row.set(field.name, toTableRowField(fieldValue, field))
      }
    }

    row
  }

  /**
   * Traverses all fields of the supplied avroSchema and converts it into
   * a TableSchema containing TableFieldSchemas.
   *
   * @param avroSchema
   * @return the equivalent BigQuery schema
   */
  @experimental
  def toTableSchema(avroSchema: Schema): TableSchema = {
    val fields = getFieldSchemas(avroSchema)

    new TableSchema().setFields(fields.asJava)
  }

  final case class AvroConversionException(
    private val message: String,
    private val cause: Throwable = null
  ) extends Exception(message, cause)
}
