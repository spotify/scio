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

import java.io.StringReader
import java.util.UUID
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method

import scala.jdk.CollectionConverters._

/** Utility for BigQuery data types. */
object BigQueryUtil {
  private lazy val jsonObjectParser = new JsonObjectParser(GsonFactory.getDefaultInstance)

  /** Parse a schema string. */
  def parseSchema(schemaString: String): TableSchema =
    jsonObjectParser
      .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

  /* Generates job ID */
  def generateJobId(projectId: String): String =
    projectId + "-" + UUID.randomUUID().toString

  private[bigquery] def isAvroFormat(format: Format[_]): Boolean =
    format == Format.GenericRecord || format == Format.GenericRecordWithLogicalTypes

  private[bigquery] def isStorageApiWrite(method: Method): Boolean =
    method == Method.STORAGE_WRITE_API || method == Method.STORAGE_API_AT_LEAST_ONCE

  private[bigquery] def containsType(schema: TableSchema, typeName: String): Boolean =
    containsType(schema.getFields.asScala, typeName)

  private[bigquery] def containsType(
    fields: Iterable[TableFieldSchema],
    typeName: String
  ): Boolean = {
    fields.exists(field =>
      field.getType match {
        case t if t == typeName  => true
        case "RECORD" | "STRUCT" => containsType(field.getFields.asScala, typeName)
        case _                   => false
      }
    )
  }
}
