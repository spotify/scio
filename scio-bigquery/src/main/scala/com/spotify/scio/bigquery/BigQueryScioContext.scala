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
import com.spotify.scio.values._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation

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
                     flattenResults: Boolean = false): SCollection[TableRow] =
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
   * Get a typed SCollection for a BigQuery SELECT query or table.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromSchema]],
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
  def typedBigQuery[T <: HasAnnotation : ClassTag : TypeTag](newSource: String = null)
    : SCollection[T] = self.read(BigQueryTyped.dynamic[T](newSource))

  /**
   * Get an SCollection for a BigQuery TableRow JSON file.
   */
  def tableRowJsonFile(path: String): SCollection[TableRow] =
    self.read(TableRowJsonIO(path))

}
