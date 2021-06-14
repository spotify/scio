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
import com.spotify.scio.bigquery.{
  BigQuerySelect,
  BigQueryStorage,
  BigQueryStorageSelect,
  Query,
  Table,
  TableRow,
  TableRowJsonIO
}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values._

import com.spotify.scio.bigquery.BigQueryTypedTable
import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import com.spotify.scio.bigquery.coders.tableRowCoder

sealed private[syntax] trait ScioContextBaseOps {
  private[syntax] val self: ScioContext

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
    bigQueryTable(table, BigQueryTypedTable.Format.TableRow)(tableRowCoder)

  /**
   * Get an SCollection for a BigQuery table using the specified [[Format]].
   *
   * Reading records as GenericRecord **should** offer better performance over
   * TableRow records.
   *
   * Note: When using `Format.GenericRecord` Bigquery types DATE, TIME and DATETIME
   *       are read as STRING.
   */
  def bigQueryTable[F: Coder](table: Table, format: Format[F]): SCollection[F] =
    self.read(BigQueryTypedTable(table, format))

  /**
   * Get an SCollection for a BigQuery table using the storage API.
   *
   * @param selectedFields names of the fields in the table that should be read. If empty, all
   *                       fields will be read. If the specified field is a nested field, all the
   *                       sub-fields in the field will be selected. Fields will always appear in
   *                       the generated class in the same order as they appear in the table,
   *                       regardless of the order specified in selectedFields.
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
    self.read(BigQueryStorage(table, selectedFields, Option(rowRestriction)))

  /**
   * Get an SCollection for a BigQuery SELECT query using the storage API.
   *
   * @param query SQL query
   */
  def bigQueryStorage(query: Query): SCollection[TableRow] =
    self.read(BigQueryStorageSelect(query))

  /** Get an SCollection for a BigQuery TableRow JSON file. */
  def tableRowJsonFile(path: String): SCollection[TableRow] =
    self.read(TableRowJsonIO(path))
}

/** Enhanced version of [[ScioContext]] with BigQuery methods. */
final class ScioContextOps(private[syntax] val self: ScioContext) extends ScioContextBaseOps

trait ScioContextSyntax {
  implicit def bigQueryScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
