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
import com.spotify.scio.bigquery.BigQueryTyped.Table.{WriteParam => TableWriteParam}
import com.spotify.scio.bigquery.BigQueryTyped.BeamSchema.{WriteParam => TypedWriteParam}
import com.spotify.scio.bigquery.TableRowJsonIO.{WriteParam => TableRowJsonWriteParam}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.{
  BigQueryTable,
  BigQueryTyped,
  TableRow,
  TableRowJsonIO,
  TimePartitioning
}
import com.spotify.scio.coders.Coder
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.spotify.scio.bigquery.Table
import com.spotify.scio.schemas.Schema
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
    schema: TableSchema = BigQueryTable.WriteParam.DefaultSchema,
    writeDisposition: WriteDisposition = BigQueryTable.WriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = BigQueryTable.WriteParam.DefaultCreateDisposition,
    tableDescription: String = BigQueryTable.WriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = BigQueryTable.WriteParam.DefaultTimePartitioning
  ): ClosedTap[TableRow] = {
    val param =
      BigQueryTable.WriteParam(
        schema,
        writeDisposition,
        createDisposition,
        tableDescription,
        timePartitioning
      )
    self
      .covary[TableRow]
      .write(BigQueryTypedTable(table, Format.TableRow))(param)
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
    schema: TableSchema = BigQueryTable.WriteParam.DefaultSchema,
    writeDisposition: WriteDisposition = BigQueryTable.WriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = BigQueryTable.WriteParam.DefaultCreateDisposition,
    tableDescription: String = BigQueryTable.WriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = BigQueryTable.WriteParam.DefaultTimePartitioning
  ): ClosedTap[GenericRecord] = {
    val param =
      BigQueryTable.WriteParam(
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

final class SCollectionBeamSchemaOps[T: ClassTag](private val self: SCollection[T]) {
  def saveAsBigQueryTable(
    table: Table,
    writeDisposition: WriteDisposition = TypedWriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = TypedWriteParam.DefaultCreateDisposition,
    tableDescription: String = TypedWriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = TypedWriteParam.DefaultTimePartitioning
  )(implicit schema: Schema[T], coder: Coder[T]): ClosedTap[T] = {
    val param =
      TypedWriteParam(
        writeDisposition,
        createDisposition,
        tableDescription,
        timePartitioning
      )
    self
      .write(BigQueryTyped.BeamSchema(table))(param)
      .asInstanceOf[ClosedTap[T]]
  }
}

/** Enhanced version of [[SCollection]] with BigQuery methods. */
final class SCollectionTypedOps[T <: HasAnnotation](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as a BigQuery table. Note that element type `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType BigQueryType]].
   *
   * This could be a complete case class with
   * [[com.spotify.scio.bigquery.types.BigQueryType.toTable BigQueryType.toTable]]. For example:
   *
   * {{{
   * @BigQueryType.toTable
   * case class Result(name: String, score: Double)
   *
   * val p: SCollection[Result] = // process data and convert elements to Result
   * p.saveAsTypedBigQueryTable(Table.Spec("myproject:mydataset.mytable"))
   * }}}
   *
   * It could also be an empty class with schema from
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromSchema]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]], or
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]. For
   * example:
   *
   * {{{
   * @BigQueryType.fromTable("bigquery-public-data:samples.gsod")
   * class Row
   *
   * sc.typedBigQuery[Row]()
   *   .sample(withReplacement = false, fraction = 0.1)
   *   .saveAsTypedBigQueryTable(Table.Spec("myproject:samples.gsod"))
   * }}}
   */
  def saveAsTypedBigQueryTable(
    table: Table,
    timePartitioning: TimePartitioning = TableWriteParam.DefaultTimePartitioning,
    writeDisposition: WriteDisposition = TableWriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = TableWriteParam.DefaultCreateDisposition
  )(implicit tt: TypeTag[T], ct: ClassTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = TableWriteParam(writeDisposition, createDisposition, timePartitioning)
    self.write(BigQueryTyped.Table[T](table))(param)
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

  implicit def bigQuerySCollectionBeamSchemaOps[T: ClassTag](
    sc: SCollection[T]
  ): SCollectionBeamSchemaOps[T] =
    new SCollectionBeamSchemaOps[T](sc)

  implicit def bigQuerySCollectionTypedOps[T <: HasAnnotation](
    sc: SCollection[T]
  ): SCollectionTypedOps[T] =
    new SCollectionTypedOps[T](sc)
}
