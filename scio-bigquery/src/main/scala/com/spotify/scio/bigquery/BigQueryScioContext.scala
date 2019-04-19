/*
 * Copyright 2016 Spotify AB.
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

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Enhanced version of [[ScioContext]] with BigQuery methods. */
final class BigQueryScioContext(@transient val self: ScioContext) extends Serializable {

  /**
   * Get an SCollection for a BigQuery SELECT query.
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def bigQuerySelect(sqlQuery: String,
                     flattenResults: Boolean = BigQuerySelect.ReadParam.DefaultFlattenResults)
    : SCollection[TableRow] =
    self.read(BigQuerySelect(sqlQuery))(BigQuerySelect.ReadParam(flattenResults))

  /**
   * Get an SCollection for a BigQuery table.
   */
  def bigQueryTable(table: TableReference): SCollection[TableRow] =
    self.read(BigQueryTable(table))

  /**
   * Get an SCollection for a BigQuery table.
   */
  def bigQueryTable(tableSpec: String): SCollection[TableRow] =
    self.read(BigQueryTable(tableSpec))

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
  def bigQueryStorage(tableSpec: String,
                      selectedFields: List[String] = Nil,
                      rowRestriction: String = null): SCollection[TableRow] =
    self.read(BigQueryStorage(tableSpec))(BigQueryStorage.ReadParam(selectedFields, rowRestriction))

  /**
   * Get an SCollection for a BigQuery table using the storage API.
   */
  def bigQueryStorage(table: TableReference,
                      selectedFields: List[String],
                      rowRestriction: String): SCollection[TableRow] =
    self.read(BigQueryStorage(table))(BigQueryStorage.ReadParam(selectedFields, rowRestriction))

  /**
   * Get a typed SCollection for a BigQuery SELECT query, table or storage.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromSchema]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromStorage]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]], or
   * [[com.spotify.scio.bigquery.types.BigQueryType.toTable BigQueryType.toTable]].
   *
   * By default the source (table or query) specified in the annotation will be used, but it can
   * be overridden with the `newSource` parameter. For example:
   *
   * {{{
   * @BigQueryType.fromTable("publicdata:samples.gsod")
   * class Row
   *
   * // Read from [publicdata:samples.gsod] as specified in the annotation.
   * sc.typedBigQuery[Row]()
   *
   * // Read from [myproject:samples.gsod] instead.
   * sc.typedBigQuery[Row]("myproject:samples.gsod")
   *
   * // Read from a query instead.
   * sc.typedBigQuery[Row]("SELECT * FROM [publicdata:samples.gsod] LIMIT 1000")
   * }}}
   *
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def typedBigQuery[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    newSource: String = null): SCollection[T] = {
    val bqt = BigQueryType[T]
    if (bqt.isStorage) {
      val table = if (newSource != null) newSource else bqt.table.get
      val params = BigQueryTyped.Storage.ReadParam(bqt.selectedFields.get, bqt.rowRestriction.get)
      self.read(BigQueryTyped.Storage(table))(params)
    } else {
      self.read(BigQueryTyped.dynamic[T](newSource))
    }
  }

  /**
   * Get an SCollection for a BigQuery TableRow JSON file.
   */
  def tableRowJsonFile(path: String): SCollection[TableRow] =
    self.read(TableRowJsonIO(path))

}
