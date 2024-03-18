/*
 * Copyright 2024 Spotify AB.
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
  BigQueryMagnolifyTypedSelectIO,
  BigQueryMagnolifyTypedStorage,
  BigQueryMagnolifyTypedStorageSelect,
  BigQueryMagnolifyTypedTable,
  BigQueryTypedTable,
  Clustering,
  Query,
  Sharding,
  Table,
  TimePartitioning
}
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import magnolify.bigquery.TableRowType
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{
  CreateDisposition,
  Method,
  WriteDisposition
}
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy
import org.joda.time.Duration

final class MagnolifyBigQueryScioContextOps(private val self: ScioContext) extends AnyVal {

  /**
   * Get an SCollection for a BigQuery SELECT query. Both
   * [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def typedBigQuerySelect[T: TableRowType: Coder](
    sqlQuery: Query,
    flattenResults: Boolean = BigQueryMagnolifyTypedSelectIO.ReadParam.DefaultFlattenResults
  ): SCollection[T] =
    self.read(BigQueryMagnolifyTypedSelectIO(sqlQuery))(
      BigQueryMagnolifyTypedSelectIO.ReadParam(flattenResults)
    )

  /** Get an SCollection for a BigQuery table. */
  def typedBigQueryTable[T: TableRowType: Coder](table: Table): SCollection[T] =
    self.read(BigQueryMagnolifyTypedTable(table))

  /**
   * Get an SCollection for a BigQuery table using the storage API.
   *
   * @param selectedFields
   *   names of the fields in the table that should be read. If empty, all fields will be read. If
   *   the specified field is a nested field, all the sub-fields in the field will be selected.
   *   Fields will always appear in the generated class in the same order as they appear in the
   *   table, regardless of the order specified in selectedFields.
   * @param rowRestriction
   *   SQL text filtering statement, similar ti a WHERE clause in a query. Currently, we support
   *   combinations of predicates that are a comparison between a column and a constant value in SQL
   *   statement. Aggregates are not supported. For example:
   *
   * {{{
   * "a > DATE '2014-09-27' AND (b > 5 AND c LIKE 'date')"
   * }}}
   */
  def typedBigQueryStorageMagnolify[T: TableRowType: Coder](
    table: Table,
    selectedFields: List[String] = BigQueryMagnolifyTypedStorage.ReadParam.DefaultSelectFields,
    rowRestriction: String = null
  ): SCollection[T] =
    self.read(BigQueryMagnolifyTypedStorage(table, selectedFields, Option(rowRestriction)))

  /**
   * Get an SCollection for a BigQuery SELECT query using the storage API.
   *
   * @param query
   *   SQL query
   */
  def typedBigQueryStorageMagnolify[T: TableRowType: Coder](query: Query): SCollection[T] =
    self.read(BigQueryMagnolifyTypedStorageSelect(query))

}

final class MagnolifyBigQuerySCollectionOps[T](private val self: SCollection[T]) {

  def saveAsBigQueryTable(
    table: Table,
    timePartitioning: TimePartitioning = BigQueryTypedTable.WriteParam.DefaultTimePartitioning,
    writeDisposition: WriteDisposition = BigQueryTypedTable.WriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = BigQueryTypedTable.WriteParam.DefaultCreateDisposition,
    clustering: Clustering = BigQueryTypedTable.WriteParam.DefaultClustering,
    method: Method = BigQueryTypedTable.WriteParam.DefaultMethod,
    triggeringFrequency: Duration = BigQueryTypedTable.WriteParam.DefaultTriggeringFrequency,
    sharding: Sharding = BigQueryTypedTable.WriteParam.DefaultSharding,
    failedInsertRetryPolicy: InsertRetryPolicy =
      BigQueryTypedTable.WriteParam.DefaultFailedInsertRetryPolicy,
    successfulInsertsPropagation: Boolean =
      BigQueryTypedTable.WriteParam.DefaultSuccessfulInsertsPropagation,
    extendedErrorInfo: Boolean = BigQueryTypedTable.WriteParam.DefaultExtendedErrorInfo,
    configOverride: BigQueryTypedTable.WriteParam.ConfigOverride[T] =
      BigQueryTypedTable.WriteParam.DefaultConfigOverride
  )(implicit coder: Coder[T], tableRowType: TableRowType[T]): ClosedTap[T] = {
    val param = BigQueryTypedTable.WriteParam[T](
      method,
      tableRowType.schema,
      writeDisposition,
      createDisposition,
      tableRowType.description,
      timePartitioning,
      clustering,
      triggeringFrequency,
      sharding,
      failedInsertRetryPolicy,
      successfulInsertsPropagation,
      extendedErrorInfo,
      configOverride
    )
    self.write(BigQueryMagnolifyTypedTable[T](table))(param)
  }

}

trait MagnolifySyntax {
  implicit def magnolifyBigQueryScioContextOps(sc: ScioContext): MagnolifyBigQueryScioContextOps =
    new MagnolifyBigQueryScioContextOps(sc)

  implicit def magnolifyBigQuerySCollectionOps[T](
    scoll: SCollection[T]
  ): MagnolifyBigQuerySCollectionOps[T] =
    new MagnolifyBigQuerySCollectionOps(scoll)
}
