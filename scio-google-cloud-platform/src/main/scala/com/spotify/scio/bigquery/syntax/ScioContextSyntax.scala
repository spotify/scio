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

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.{
  BigQuerySelect,
  BigQueryStorage,
  BigQueryStorageSelect,
  BigQueryType,
  BigQueryTyped,
  Query,
  Source,
  Table,
  TableRow,
  TableRowJsonIO
}
import com.spotify.scio.coders.Coder
import com.spotify.scio.schemas.Schema
import com.spotify.scio.values._
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import com.spotify.scio.bigquery.BigQueryTypedTable
import com.spotify.scio.bigquery.BigQueryTypedTable.Format

/** Enhanced version of [[ScioContext]] with BigQuery methods. */
final class ScioContextOps(private val self: ScioContext) extends AnyVal {

  /**
   * Get an SCollection for a BigQuery SELECT query.
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def bigQuerySelect(
    sqlQuery: Query,
    flattenResults: Boolean
  ): SCollection[TableRow] =
    self.read(BigQuerySelect(sqlQuery))(BigQuerySelect.ReadParam(flattenResults))

  /**
   * Get an SCollection for a BigQuery SELECT query.
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def bigQuerySelect(
    sqlQuery: Query
  ): SCollection[TableRow] =
    bigQuerySelect(sqlQuery, BigQuerySelect.ReadParam.DefaultFlattenResults)

  /** Get an SCollection for a BigQuery table. */
  def bigQueryTable(table: Table): SCollection[TableRow] =
    bigQueryTable(table, BigQueryTypedTable.Format.TableRow)

  /**
   * Get an SCollection for a BigQuery table using the specified [[Format]].
   *
   * Reading records as GenericRecord **should** offer better performance over
   * TableRow records.
   *
   * Note: When using `Format.GenericRecord` Bigquery types DATE, TIME and DATETIME
   *       are read as STRING.
   */
  def bigQueryTable(table: Table, format: Format): SCollection[format.F] =
    self.read(BigQueryTypedTable(table, format))

  /**
   * Get an SCollection for a BigQuery table using the storage API.
   *
   * @param selectedFields names of the fields in the table that should be read. If empty, all
   *                       fields will be read. If the specified field is a nested field, all the
   *                       sub-fields in the field will be selected.
   * @param rowRestriction SQL text filtering statement, similar ti a WHERE clause in a query.
   *                       Currently, we support combinations of predicates that are a comparison
   *                       between a column and a constant value in SQL statement. Aggregates are
   *                       not supported. For example:
   *
   * {{{
   * "a > DATE '2014-09-27' AND (b > 5 AND c LIKE 'date')"
   * }}}
   */
  def bigQueryStorage(
    table: Table,
    selectedFields: List[String] = BigQueryStorage.ReadParam.DefaultSelectFields,
    rowRestriction: String = null
  ): SCollection[TableRow] =
    self.read(BigQueryStorage(table))(
      BigQueryStorage.ReadParam(selectedFields, Option(rowRestriction))
    )

  /**
   * Get an SCollection for a BigQuery SELECT query using the storage API.
   *
   * @param query SQL query
   */
  def bigQueryStorage(query: Query): SCollection[TableRow] =
    self.read(BigQueryStorageSelect(query))

  def typedBigQuery[T <: HasAnnotation: ClassTag: TypeTag: Coder](): SCollection[T] =
    typedBigQuery(None)

  def typedBigQuery[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    newSource: Source
  ): SCollection[T] = typedBigQuery(Option(newSource))

  def typedBigQuery[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    newSource: Option[Source]
  ): SCollection[T] = {
    val bqt = BigQueryType[T]
    if (bqt.isStorage) {
      newSource
        .asInstanceOf[Option[Table]]
        .map(typedBigQueryStorage(_))
        .getOrElse(typedBigQueryStorage())
    } else {
      self.read(BigQueryTyped.dynamic[T](newSource))
    }
  }

  def typedBigQueryTable[T: Schema: Coder: ClassTag](table: Table): SCollection[T] =
    self.read(BigQueryTyped.BeamSchema(table))

  def typedBigQueryTable[T: Schema: Coder: ClassTag](
    table: Table,
    parseFn: SchemaAndRecord => T
  ): SCollection[T] =
    self.read(BigQueryTyped.BeamSchema(table, parseFn))

  /**
   * Get a typed SCollection for a BigQuery storage API.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromStorage]] or
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]
   */
  def typedBigQueryStorage[T <: HasAnnotation: ClassTag: TypeTag: Coder](): SCollection[T] = {
    val bqt = BigQueryType[T]
    if (bqt.isQuery) {
      self.read(BigQueryTyped.StorageQuery[T](Query(bqt.query.get)))
    } else {
      val table = Table.Spec(bqt.table.get)
      val rr = bqt.rowRestriction
      val fields = bqt.selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields)
      val params = BigQueryTyped.Storage.ReadParam(fields, rr)
      self.read(BigQueryTyped.Storage[T](table))(params)
    }
  }

  def typedBigQueryStorage[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    table: Table
  ): SCollection[T] =
    self.read(BigQueryTyped.Storage[T](table))(
      BigQueryStorage.ReadParam(
        BigQueryType[T].selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields),
        BigQueryType[T].rowRestriction
      )
    )

  def typedBigQueryStorage[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    rowRestriction: String
  ): SCollection[T] = {
    val bqt = BigQueryType[T]
    val table = Table.Spec(bqt.table.get)
    self.read(BigQueryTyped.Storage[T](table))(
      BigQueryTyped.Storage.ReadParam(
        bqt.selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields),
        Option(rowRestriction)
      )
    )
  }

  def typedBigQueryStorage[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    table: Table,
    rowRestriction: String
  ): SCollection[T] =
    self.read(BigQueryTyped.Storage[T](table))(
      BigQueryTyped.Storage.ReadParam(
        BigQueryType[T].selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields),
        Option(rowRestriction)
      )
    )

  /** Get an SCollection for a BigQuery TableRow JSON file. */
  def tableRowJsonFile(path: String): SCollection[TableRow] =
    self.read(TableRowJsonIO(path))
}

trait ScioContextSyntax {
  implicit def bigQueryScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
