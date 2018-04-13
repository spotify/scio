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

package com.spotify.scio.bigquery.io

import scala.concurrent.Future

import com.spotify.scio.io.{Tap, Taps, FileStorage}
import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.bigquery._
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.language.implicitConversions


/** Tap for BigQuery TableRow JSON files on local file system or GCS. */
case class TableRowJsonTap(path: String) extends Tap[TableRow] {
  override def value: Iterator[TableRow] = FileStorage(path).tableRowJsonFile
  override def open(sc: ScioContext): SCollection[TableRow] = sc.tableRowJsonFile(path)
}

/** Tap for BigQuery tables. */
case class BigQueryTap(table: TableReference) extends Tap[TableRow] {
  override def value: Iterator[TableRow] = BigQueryClient.defaultInstance().getTableRows(table)
  override def open(sc: ScioContext): SCollection[TableRow] = sc.bigQueryTable(table)
}

final case class BigQueryTaps[T <: Taps](self: T) {
  import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
  import com.spotify.scio.bigquery.types.BigQueryType
  import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
  import com.spotify.scio.bigquery.{BigQueryClient, TableRow}
  import self.mkTap

  private lazy val bqc = BigQueryClient.defaultInstance()

  /** Get a `Future[Tap[TableRow]]` for BigQuery SELECT query. */
  def bigQuerySelect(sqlQuery: String, flattenResults: Boolean = false): Future[Tap[TableRow]] =
    mkTap(
      s"BigQuery SELECT: $sqlQuery",
      () => isQueryDone(sqlQuery),
      () => bigQueryTap(sqlQuery, flattenResults))

  /** Get a `Future[Tap[TableRow]]` for BigQuery table. */
  def bigQueryTable(table: TableReference): Future[Tap[TableRow]] =
    mkTap(s"BigQuery Table: $table", () => bqc.tableExists(table), () => BigQueryTap(table))

  /** Get a `Future[Tap[TableRow]]` for BigQuery table. */
  def bigQueryTable(tableSpec: String): Future[Tap[TableRow]] =
    bigQueryTable(BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get a `Future[Tap[T]]` for typed BigQuery source. */
  def typedBigQuery[T <: HasAnnotation : TypeTag : ClassTag](newSource: String = null)
  : Future[Tap[T]] = {
    val bqt = BigQueryType[T]
    val rows = if (newSource == null) {
      // newSource is missing, T's companion object must have either table or query
      if (bqt.isTable) {
        bigQueryTable(bqt.table.get)
      } else if (bqt.isQuery) {
        bigQuerySelect(bqt.query.get)
      } else {
        throw new IllegalArgumentException(s"Missing table or query field in companion object")
      }
    } else {
      // newSource can be either table or query
      val table = scala.util.Try(BigQueryHelpers.parseTableSpec(newSource)).toOption
      if (table.isDefined) {
        bigQueryTable(table.get)
      } else {
        bigQuerySelect(newSource)
      }
    }
    import scala.concurrent.ExecutionContext.Implicits.global
    rows.map(_.map(bqt.fromTableRow))
  }

  /** Get a `Future[Tap[TableRow]]` for a BigQuery TableRow JSON file. */
  def tableRowJsonFile(path: String): Future[Tap[TableRow]] =
    mkTap(s"TableRowJson: $path", () => self.isPathDone(path), () => TableRowJsonTap(path))

  private def isQueryDone(sqlQuery: String): Boolean =
    bqc.extractTables(sqlQuery).forall(bqc.tableExists)

  private def bigQueryTap(sqlQuery: String, flattenResults: Boolean): BigQueryTap = {
    val table = bqc.query(sqlQuery, flattenResults = flattenResults)
    BigQueryTap(table)
  }
}

object BigQueryTaps {
  implicit def toBigQueryTaps[T <: Taps](underlying: T): BigQueryTaps[T] =
    BigQueryTaps(underlying)
}
