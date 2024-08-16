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
import com.spotify.scio.bigquery.{BigQueryIO, Query, Source, Table, TableRow, TableRowJsonIO}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values._

import scala.reflect.runtime.universe._
import com.spotify.scio.bigquery.coders.tableRowCoder
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.apache.beam.sdk.transforms.errorhandling.{BadRecord, ErrorHandler}
import org.slf4j.{Logger, LoggerFactory}

object ScioContextOps {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
}

/** Enhanced version of [[ScioContext]] with BigQuery methods. */
final class ScioContextOps(private val self: ScioContext) extends AnyVal {
  import ScioContextOps._

  /**
   * Get an SCollection for a BigQuery SELECT query. Both
   * [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def bigQuerySelect(
    sqlQuery: Query,
    flattenResults: Boolean = BigQueryIO.ReadParam.DefaultFlattenResults,
    configOverride: BigQueryIO.ReadParam.ConfigOverride[TableRow] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[TableRow] = {
    val params = BigQueryIO.QueryReadParam(
      BigQueryIO.Format.Default(),
      Method.DEFAULT,
      flattenResults,
      BigQueryIO.ReadParam.DefaultErrorHandler,
      configOverride
    )
    self.read(BigQueryIO[TableRow](sqlQuery))(params)
  }

  def bigQuerySelectFormat[T: Coder](
    sqlQuery: Query,
    format: BigQueryIO.Format[T],
    flattenResults: Boolean = BigQueryIO.ReadParam.DefaultFlattenResults,
    configOverride: BigQueryIO.ReadParam.ConfigOverride[T] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[T] = {
    val params = BigQueryIO.QueryReadParam(
      format,
      Method.DEFAULT,
      flattenResults,
      BigQueryIO.ReadParam.DefaultErrorHandler,
      configOverride
    )
    self.read(BigQueryIO[T](sqlQuery))(params)
  }

  def bigQueryTable(
    table: Table,
    configOverride: BigQueryIO.ReadParam.ConfigOverride[TableRow] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[TableRow] = {
    if (table.filter.nonEmpty) {
      logger.warn(
        "Using filtered table with standard API. " +
          "selectedFields and rowRestriction are ignored. " +
          "Use bigQueryStorage instead"
      )
    }
    val params = BigQueryIO.TableReadParam(
      BigQueryIO.Format.Default(),
      Method.DEFAULT,
      BigQueryIO.ReadParam.DefaultErrorHandler,
      configOverride
    )
    self.read(BigQueryIO[TableRow](table))(params)
  }

  def bigQueryTableFormat[T: Coder](
    table: Table,
    format: BigQueryIO.Format[T],
    configOverride: BigQueryIO.ReadParam.ConfigOverride[T] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[T] = {
    if (table.filter.nonEmpty) {
      logger.warn(
        "Using filtered table with standard API. " +
          "selectedFields and rowRestriction are ignored. " +
          "Use bigQueryStorage instead"
      )
    }
    val params = BigQueryIO.TableReadParam(
      format,
      Method.DEFAULT,
      BigQueryIO.ReadParam.DefaultErrorHandler,
      configOverride
    )
    self.read(BigQueryIO[T](table))(params)
  }

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
  def bigQueryStorage(
    table: Table,
    errorHandler: ErrorHandler[BadRecord, _] = BigQueryIO.ReadParam.DefaultErrorHandler,
    configOverride: BigQueryIO.ReadParam.ConfigOverride[TableRow] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[TableRow] = {
    val params = BigQueryIO.TableReadParam(
      BigQueryIO.Format.Default(),
      Method.DIRECT_READ,
      errorHandler,
      configOverride
    )
    self.read(BigQueryIO[TableRow](table))(params)
  }

  def bigQueryStorageFormat[T: Coder](
    table: Table,
    format: BigQueryIO.Format[T],
    errorHandler: ErrorHandler[BadRecord, _] = BigQueryIO.ReadParam.DefaultErrorHandler,
    configOverride: BigQueryIO.ReadParam.ConfigOverride[T] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[T] = {
    val params = BigQueryIO.TableReadParam(
      format,
      Method.DIRECT_READ,
      errorHandler,
      configOverride
    )
    self.read(BigQueryIO[T](table))(params)
  }

  /**
   * Get an SCollection for a BigQuery SELECT query using the storage API.
   *
   * @param query
   *   SQL query
   */
  def bigQuerySelectStorage(
    query: Query,
    flattenResults: Boolean = BigQueryIO.ReadParam.DefaultFlattenResults,
    errorHandler: ErrorHandler[BadRecord, _] = BigQueryIO.ReadParam.DefaultErrorHandler,
    configOverride: BigQueryIO.ReadParam.ConfigOverride[TableRow] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[TableRow] = {
    val params = BigQueryIO.QueryReadParam(
      BigQueryIO.Format.Default(),
      Method.DIRECT_READ,
      flattenResults,
      errorHandler,
      configOverride
    )
    self.read(BigQueryIO[TableRow](query))(params)
  }

  def bigQuerySelectStorageFormat[T: Coder](
    query: Query,
    format: BigQueryIO.Format[T],
    flattenResults: Boolean = BigQueryIO.ReadParam.DefaultFlattenResults,
    errorHandler: ErrorHandler[BadRecord, _] = BigQueryIO.ReadParam.DefaultErrorHandler,
    configOverride: BigQueryIO.ReadParam.ConfigOverride[T] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[T] = {
    val params = BigQueryIO.QueryReadParam(
      format,
      Method.DIRECT_READ,
      flattenResults,
      errorHandler,
      configOverride
    )
    self.read(BigQueryIO[T](query))(params)
  }

  /** Get a typed SCollection for BigQuery Table or a SELECT query. */
  def typedBigQuery[T <: HasAnnotation: TypeTag: Coder](
    source: Source = null,
    configOverride: BigQueryIO.ReadParam.ConfigOverride[T] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[T] = {
    val format = BigQueryIO.Format.Avro(BigQueryType[T])
    val io = Option(source) match {
      case Some(s) => BigQueryIO[T](s)
      case None    => BigQueryIO[T]
    }

    val params = io.source match {
      case _: Query =>
        BigQueryIO.QueryReadParam[T](
          format,
          Method.DEFAULT,
          BigQueryIO.ReadParam.DefaultFlattenResults,
          BigQueryIO.ReadParam.DefaultErrorHandler,
          configOverride
        )
      case t: Table =>
        if (t.filter.nonEmpty) {
          logger.warn(
            "Using filtered table with standard API. " +
              "selectedFields and rowRestriction are ignored. " +
              "Use typedBigQueryStorage instead"
          )
        }
        BigQueryIO.TableReadParam[T](
          format,
          Method.DEFAULT,
          BigQueryIO.ReadParam.DefaultErrorHandler,
          configOverride
        )
    }
    self.read(io)(params)
  }

  /** Get a typed SCollection for a BigQuery storage API. */
  def typedBigQueryStorage[T <: HasAnnotation: TypeTag: Coder](
    source: Source = null,
    errorHandler: ErrorHandler[BadRecord, _] = BigQueryIO.ReadParam.DefaultErrorHandler,
    configOverride: BigQueryIO.ReadParam.ConfigOverride[T] =
      BigQueryIO.ReadParam.DefaultConfigOverride
  ): SCollection[T] = {
    val io = Option(source) match {
      case Some(s) => BigQueryIO[T](s)
      case None    => BigQueryIO[T]
    }

    val format = BigQueryIO.Format.Avro(BigQueryType[T])
    val params = io.source match {
      case _: Query =>
        BigQueryIO.QueryReadParam[T](
          format,
          Method.DIRECT_READ,
          BigQueryIO.ReadParam.DefaultFlattenResults,
          errorHandler,
          configOverride
        )
      case _: Table =>
        BigQueryIO.TableReadParam[T](format, Method.DIRECT_READ, errorHandler, configOverride)
    }

    self.read(io)(params)
  }

  /** Get an SCollection for a BigQuery TableRow JSON file. */
  def tableRowJsonFile(
    path: String,
    compression: Compression = TableRowJsonIO.ReadParam.DefaultCompression,
    emptyMatchTreatment: EmptyMatchTreatment = TableRowJsonIO.ReadParam.DefaultEmptyMatchTreatment,
    suffix: String = TableRowJsonIO.ReadParam.DefaultSuffix
  ): SCollection[TableRow] = {
    self.read(TableRowJsonIO(path))(
      TableRowJsonIO.ReadParam(compression, emptyMatchTreatment, suffix)
    )
  }
}

trait ScioContextSyntax {
  implicit def bigQueryScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
