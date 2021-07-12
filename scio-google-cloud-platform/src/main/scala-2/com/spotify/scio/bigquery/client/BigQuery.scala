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

package com.spotify.scio.bigquery.client

import com.google.api.services.bigquery.model._
import com.spotify.scio.bigquery.{Table => STable}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.{BigQueryType, CREATE_IF_NEEDED, WRITE_EMPTY}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.{bigquery => beam}

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

trait TypedBigQuery {
  self: BigQuery =>

  // =======================================================================
  // Type safe API
  // =======================================================================

  /**
   * Get a typed iterator for a BigQuery SELECT query or table.
   *
   * Note that `T` must be annotated with [[BigQueryType.fromSchema]],
   * [[BigQueryType.fromTable]], [[BigQueryType.fromQuery]], or [[BigQueryType.toTable]].
   *
   * By default the source (table or query) specified in the annotation will be used, but it can
   * be overridden with the `newSource` parameter. For example:
   *
   * {{{
   * @BigQueryType.fromTable("bigquery-public-data:samples.gsod")
   * class Row
   *
   * // Read from [bigquery-public-data:samples.gsod] as specified in the annotation.
   * bq.getTypedRows[Row]()
   *
   * // Read from [myproject:samples.gsod] instead.
   * bq.getTypedRows[Row]("myproject:samples.gsod")
   *
   * // Read from a query instead.
   * bq.getTypedRows[Row]("SELECT * FROM [bigquery-public-data:samples.gsod] LIMIT 1000")
   * }}}
   */
  def getTypedRows[T <: HasAnnotation: TypeTag](newSource: String = null): Iterator[T] = {
    val bqt = BigQueryType[T]
    val rows = if (newSource == null) {
      // newSource is missing, T's companion object must have either table or query
      if (bqt.isTable) {
        tables.rows(STable.Spec(bqt.table.get))
      } else if (bqt.isQuery) {
        query.rows(bqt.query.get)
      } else {
        throw new IllegalArgumentException("Missing table or query field in companion object")
      }
    } else {
      // newSource can be either table or query
      Try(BigQueryHelpers.parseTableSpec(newSource)).toOption
        .map(STable.Ref)
        .map(tables.rows)
        .getOrElse(query.rows(newSource))
    }
    rows.map(bqt.fromTableRow)
  }

  /**
   * Write a List of rows to a BigQuery table. Note that element type `T` must be annotated with
   * [[BigQueryType]].
   */
  def writeTypedRows[T <: HasAnnotation: TypeTag](
    table: TableReference,
    rows: List[T],
    writeDisposition: WriteDisposition,
    createDisposition: CreateDisposition
  ): Long = {
    val bqt = BigQueryType[T]
    tables.writeRows(
      table,
      rows.map(bqt.toTableRow),
      bqt.schema,
      writeDisposition,
      createDisposition
    )
  }

  /**
   * Write a List of rows to a BigQuery table. Note that element type `T` must be annotated with
   * [[BigQueryType]].
   */
  def writeTypedRows[T <: HasAnnotation: TypeTag](
    tableSpec: String,
    rows: List[T],
    writeDisposition: WriteDisposition = WRITE_EMPTY,
    createDisposition: CreateDisposition = CREATE_IF_NEEDED
  ): Long =
    writeTypedRows(
      beam.BigQueryHelpers.parseTableSpec(tableSpec),
      rows,
      writeDisposition,
      createDisposition
    )

  def createTypedTable[T <: HasAnnotation: TypeTag](table: Table): Unit =
    tables.create(table.setSchema(BigQueryType[T].schema))

  def createTypedTable[T <: HasAnnotation: TypeTag](table: TableReference): Unit =
    tables.create(table, BigQueryType[T].schema)

  def createTypedTable[T <: HasAnnotation: TypeTag](tableSpec: String): Unit =
    createTypedTable(beam.BigQueryHelpers.parseTableSpec(tableSpec))
}
