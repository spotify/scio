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

package com.spotify.scio.bigquery

import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.BigQueryIO.Format
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{FileStorage, Tap, Taps}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method

import scala.concurrent.Future
import scala.reflect.runtime.universe._
import scala.jdk.CollectionConverters._

/** Tap for BigQuery TableRow JSON files. */
final case class TableRowJsonTap(path: String, params: TableRowJsonIO.ReadParam)
    extends Tap[TableRow] {
  override def value: Iterator[TableRow] =
    FileStorage(path, params.suffix).tableRowJsonFile
  override def open(sc: ScioContext): SCollection[TableRow] =
    sc.read(TableRowJsonIO(path))(params)
}

/** Tap for BigQuery tables. */
final case class BigQueryTap[T: Coder](table: Table, params: BigQueryIO.ReadParam[T])
    extends Tap[T] {
  override def value: Iterator[T] = {
    val tables = BigQuery.defaultInstance().tables
    params.format match {
      case f: Format.Default[T] => tables.rows(table).map(f.from)
      case f: Format.Avro[T]    => tables.avroRows(table).map(f.from)
    }
  }

  override def open(sc: ScioContext): SCollection[T] =
    sc.read(BigQueryIO[T](table))(params)
}

final case class BigQueryTaps(self: Taps) {
  import com.spotify.scio.bigquery.types.BigQueryType
  import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
  import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
  import self.mkTap

  private lazy val bqc = BigQuery.defaultInstance()

  /** Get a `Future[Tap[TableRow]]` for BigQuery SELECT query. */
  def bigQuerySelect(sqlQuery: String): Future[Tap[TableRow]] =
    mkTap(
      s"BigQuery SELECT: $sqlQuery",
      () => isQueryDone(sqlQuery),
      () =>
        BigQueryIO[TableRow](Query(sqlQuery))
          .tap(BigQueryIO.QueryReadParam(BigQueryIO.Format.Default(), Method.DEFAULT))
    )

  /** Get a `Future[Tap[TableRow]]` for BigQuery table. */
  def bigQueryTable(table: TableReference): Future[Tap[TableRow]] =
    mkTap(
      s"BigQuery Table: $table",
      () => bqc.tables.exists(table),
      () =>
        BigQueryIO[TableRow](Table(table))
          .tap(BigQueryIO.TableReadParam(BigQueryIO.Format.Default(), Method.DEFAULT))
    )

  /** Get a `Future[Tap[TableRow]]` for BigQuery table. */
  def bigQueryTable(tableSpec: String): Future[Tap[TableRow]] =
    bigQueryTable(BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get a `Future[Tap[T]]` for typed BigQuery source. */
  def typedBigQuery[T <: HasAnnotation: TypeTag: Coder](
    newSource: Option[Source] = None
  ): Future[Tap[T]] = {
    val bqt = BigQueryType[T]
    val rows = newSource match {
      case Some(q: Query)      => bigQuerySelect(q.underlying)
      case Some(t: Table)      => bigQueryTable(t.ref)
      case None if bqt.isQuery => bigQuerySelect(bqt.queryRaw.get)
      case _                   => bigQueryTable(bqt.table.get)
    }
    import scala.concurrent.ExecutionContext.Implicits.global
    rows.map(_.map(bqt.fromTableRow))
  }

  /** Get a `Future[Tap[TableRow]]` for a BigQuery TableRow JSON file. */
  def tableRowJsonFile(
    path: String,
    params: TableRowJsonIO.ReadParam = TableRowJsonIO.ReadParam()
  ): Future[Tap[TableRow]] =
    mkTap(
      s"TableRowJson: $path",
      () => self.isPathDone(path, params.suffix),
      () => TableRowJsonTap(path, params)
    )

  def bigQueryStorage(
    tableSpec: String,
    readOptions: TableReadOptions
  ): Future[Tap[TableRow]] =
    bigQueryStorage(BigQueryHelpers.parseTableSpec(tableSpec), readOptions)

  def bigQueryStorage(
    table: TableReference,
    readOptions: TableReadOptions
  ): Future[Tap[TableRow]] =
    mkTap(
      s"BigQuery direct read table: $table",
      () => bqc.tables.exists(table),
      () => {
        val format = BigQueryIO.Format.Default()
        val selectedFields = readOptions.getSelectedFieldsList.asScala.toList
        val rowRestriction = Option(readOptions.getRowRestriction)
        val filter = Table.Filter(selectedFields, rowRestriction)
        val source = Table(table, filter)
        BigQueryIO[TableRow](source).tap(BigQueryIO.TableReadParam(format, Method.DIRECT_READ))
      }
    )

  def typedBigQueryStorage[T: TypeTag: Coder](
    table: TableReference,
    readOptions: TableReadOptions
  ): Future[Tap[T]] = {
    mkTap(
      s"BigQuery direct read table: $table",
      () => bqc.tables.exists(table),
      () => {
        val format = BigQueryIO.Format.Avro(BigQueryType[T])
        val selectedFields = readOptions.getSelectedFieldsList.asScala.toList
        val rowRestriction = Option(readOptions.getRowRestriction)
        val filter = Table.Filter(selectedFields, rowRestriction)
        val source = Table(table, filter)
        BigQueryIO[T](source).tap(BigQueryIO.TableReadParam(format, Method.DIRECT_READ))
      }
    )
  }

  private def isQueryDone(sqlQuery: String): Boolean =
    bqc.query.extractTables(sqlQuery).forall(bqc.tables.exists)
}

object BigQueryTaps {
  implicit def toBigQueryTaps(underlying: Taps): BigQueryTaps =
    BigQueryTaps(underlying)
}
