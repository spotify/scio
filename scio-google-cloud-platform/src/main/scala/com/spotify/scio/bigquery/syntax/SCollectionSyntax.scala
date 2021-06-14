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

package com.spotify.scio.bigquery.syntax

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.bigquery.TableRowJsonIO.{WriteParam => TableRowJsonWriteParam}
import com.spotify.scio.bigquery.{TableRow, TableRowJsonIO, TimePartitioning}
import com.spotify.scio.coders.Coder
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

import com.spotify.scio.bigquery.Table
import com.spotify.scio.bigquery.BigQueryTypedTable
import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import org.apache.avro.generic.GenericRecord

/** Enhanced version of [[SCollection]] with BigQuery methods. */
final class SCollectionTableRowOps[T <: TableRow](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection as a BigQuery table. Note that elements must be of type
   * [[com.google.api.services.bigquery.model.TableRow TableRow]].
   */
  def saveAsBigQueryTable(
    table: Table,
    schema: TableSchema = BigQueryTypedTable.WriteParam.DefaultSchema,
    writeDisposition: WriteDisposition = BigQueryTypedTable.WriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = BigQueryTypedTable.WriteParam.DefaultCreateDisposition,
    tableDescription: String = BigQueryTypedTable.WriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = BigQueryTypedTable.WriteParam.DefaultTimePartitioning
  ): ClosedTap[TableRow] = {
    val param =
      BigQueryTypedTable.WriteParam(
        schema,
        writeDisposition,
        createDisposition,
        tableDescription,
        timePartitioning
      )

    self
      .covary[TableRow]
      .write(
        BigQueryTypedTable(table, Format.TableRow)(
          self.coder.asInstanceOf[Coder[TableRow]]
        )
      )(param)
  }

  /**
   * Save this SCollection as a BigQuery TableRow JSON text file. Note that elements must be of
   * type [[com.google.api.services.bigquery.model.TableRow TableRow]].
   */
  def saveAsTableRowJsonFile(
    path: String,
    numShards: Int = TableRowJsonWriteParam.DefaultNumShards,
    compression: Compression = TableRowJsonWriteParam.DefaultCompression
  ): ClosedTap[TableRow] = {
    val param = TableRowJsonWriteParam(numShards, compression)
    self.covary[TableRow].write(TableRowJsonIO(path))(param)
  }
}

/** Enhanced version of [[SCollection]] with BigQuery methods */
final class SCollectionGenericRecordOps[T <: GenericRecord](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as a BigQuery table using Avro writting function.
   * Note that elements must be of type
   * [[org.apache.avro.generic.GenericRecord GenericRecord]].
   */
  def saveAsBigQueryTable(
    table: Table,
    schema: TableSchema = BigQueryTypedTable.WriteParam.DefaultSchema,
    writeDisposition: WriteDisposition = BigQueryTypedTable.WriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = BigQueryTypedTable.WriteParam.DefaultCreateDisposition,
    tableDescription: String = BigQueryTypedTable.WriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = BigQueryTypedTable.WriteParam.DefaultTimePartitioning
  ): ClosedTap[GenericRecord] = {
    val param =
      BigQueryTypedTable.WriteParam(
        schema,
        writeDisposition,
        createDisposition,
        tableDescription,
        timePartitioning
      )
    self
      .covary[GenericRecord]
      .write(
        BigQueryTypedTable(table, Format.GenericRecord)(
          self.coder.asInstanceOf[Coder[GenericRecord]]
        )
      )(param)
  }

}

trait SCollectionSyntax {
  implicit def bigQuerySCollectionTableRowOps[T <: TableRow](
    sc: SCollection[T]
  ): SCollectionTableRowOps[T] =
    new SCollectionTableRowOps[T](sc)

  implicit def bigQuerySCollectionGenericRecordOps[T <: GenericRecord](
    sc: SCollection[T]
  ): SCollectionGenericRecordOps[T] =
    new SCollectionGenericRecordOps[T](sc)
}
