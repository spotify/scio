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

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.bigquery.BigQueryTable.WriteParam
import com.spotify.scio.bigquery._
import com.spotify.scio.io.Tap
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

import scala.concurrent.Future
import scala.reflect.ClassTag

/** Provides implicit helpers for SCollections interacting with BigQuery. */
object Implicits extends ToTableRow with ToTableSchema {

  final case class AvroConversionException(
    private val message: String,
    private val cause: Throwable = null
  ) extends Exception(message, cause)

  implicit class AvroImplicits[T](private val self: SCollection[T]) {

    /**
     * Saves the provided SCollection[T] to BigQuery where T is a subtype of Indexed Record,
     * automatically converting T's [[org.apache.avro.Schema AvroSchema]] to BigQuery's
     * [[com.google.api.services.bigquery.model.TableSchema TableSchema]] and converting each
     * [[org.apache.avro.generic.IndexedRecord IndexedRecord]] into a
     * [[com.spotify.scio.bigquery.TableRow TableRow]].
     */
    def saveAvroAsBigQuery(table: TableReference,
                           avroSchema: Schema = null,
                           writeDisposition: WriteDisposition = null,
                           createDisposition: CreateDisposition = null,
                           tableDescription: String = null)(
      implicit ev: T <:< IndexedRecord,
      c: ClassTag[T]): Future[Tap[TableRow]] = {
      val schema: Schema = Option(avroSchema)
        .getOrElse {
          val cls = ScioUtil.classOf[T]
          if (classOf[IndexedRecord] isAssignableFrom cls) {
            cls.getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]
          } else {
            throw AvroConversionException("Could not invoke $SCHEMA on provided Avro type")
          }
        }

      val params =
        WriteParam(toTableSchema(schema), writeDisposition, createDisposition, tableDescription)
      self
        .map(toTableRow(_))
        .write(BigQueryTable(table))(params)
    }
  }

}
