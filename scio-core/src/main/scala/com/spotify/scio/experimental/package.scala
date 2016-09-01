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

package com.spotify.scio

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.dataflow.sdk.io.BigQueryIO
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Main package for experimental APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.experimental._
 * }}}
 */
package object experimental {

  /** Typed BigQuery annotations and converters. */
  val BigQueryType = com.spotify.scio.bigquery.types.BigQueryType

  /** Enhanced version of [[ScioContext]] with experimental features. */
  // TODO: scala 2.11
  // implicit class ExperimentalScioContext(private val self: ScioContext) extends AnyVal {
  implicit class ExperimentalScioContext(val self: ScioContext) {

    /**
     * Get a typed SCollection for a BigQuery SELECT query or table.
     *
     * Note that `T` must be annotated with [[BigQueryType.fromSchema]],
     * [[BigQueryType.fromTable]], [[BigQueryType.fromQuery]], or [[BigQueryType.toTable]].
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
     */
    def typedBigQuery[T <: HasAnnotation : ClassTag : TypeTag](newSource: String = null)
    : SCollection[T] = {
      val bqt = BigQueryType[T]
      val rows = if (newSource == null) {
        // newSource is missing, T's companion object must have either table or query
        if (bqt.isTable) {
          self.bigQueryTable(bqt.table.get)
        } else if (bqt.isQuery) {
          self.bigQuerySelect(bqt.query.get)
        } else {
          throw new IllegalArgumentException(s"Missing table or query field in companion object")
        }
      } else {
        // newSource can be either table or query
        val table = scala.util.Try(BigQueryIO.parseTableSpec(newSource)).toOption
        if (table.isDefined) {
          self.bigQueryTable(table.get)
        } else {
          self.bigQuerySelect(newSource)
        }
      }
      rows.map(bqt.fromTableRow)
    }

  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with
   * experimental features.
   */
  // TODO: scala 2.11
  // implicit class ExperimentalSCollection[T](private val self: SCollection[T]) extends AnyVal {
  implicit class ExperimentalSCollection[T](val self: SCollection[T]) {

    /**
     * Save this SCollection as a BigQuery table. Note that element type `T` must be a case class
     * annotated with [[BigQueryType.toTable]].
     */
    def saveAsTypedBigQuery(table: TableReference,
                            writeDisposition: WriteDisposition,
                            createDisposition: CreateDisposition)
                           (implicit ct: ClassTag[T], tt: TypeTag[T], ev: T <:< HasAnnotation)
    : Future[Tap[T]] = {
      val bqt = BigQueryType[T]
      import scala.concurrent.ExecutionContext.Implicits.global
      self
        .map(bqt.toTableRow)
        .saveAsBigQuery(table, bqt.schema, writeDisposition, createDisposition)
        .map(_.map(bqt.fromTableRow))
    }

    /**
     * Save this SCollection as a BigQuery table. Note that element type `T` must be annotated with
     * [[BigQueryType]].
     *
     * This could be a complete case class with [[BigQueryType.toTable]]. For example:
     *
     * {{{
     * @BigQueryType.toTable
     * case class Result(name: String, score: Double)
     *
     * val p: SCollection[Result] = // process data and convert elements to Result
     * p.saveAsTypedBigQuery("myproject:mydataset.mytable")
     * }}}
     *
     * It could also be an empty class with schema from [[BigQueryType.fromSchema]],
     * [[BigQueryType.fromTable]], or [[BigQueryType.fromQuery]]. For example:
     *
     * {{{
     * @BigQueryType.fromTable("publicdata:samples.gsod")
     * class Row
     *
     * sc.typedBigQuery[Row]()
     *   .sample(withReplacement = false, fraction = 0.1)
     *   .saveAsTypedBigQuery("myproject:samples.gsod")
     * }}}
     */
    def saveAsTypedBigQuery(tableSpec: String,
                            writeDisposition: WriteDisposition = null,
                            createDisposition: CreateDisposition = null)
                           (implicit ct: ClassTag[T], tt: TypeTag[T], ev: T <:< HasAnnotation)
    : Future[Tap[T]] =
      saveAsTypedBigQuery(BigQueryIO.parseTableSpec(tableSpec), writeDisposition, createDisposition)

  }

  /** Enhanced version of [[BigQueryClient]] with type-safe features. */
  implicit class TypedBigQueryClient(self: BigQueryClient) {

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
     * @BigQueryType.fromTable("publicdata:samples.gsod")
     * class Row
     *
     * // Read from [publicdata:samples.gsod] as specified in the annotation.
     * bq.getTypedRows[Row]()
     *
     * // Read from [myproject:samples.gsod] instead.
     * bq.getTypedRows[Row]("myproject:samples.gsod")
     *
     * // Read from a query instead.
     * sc.getTypedRows[Row]("SELECT * FROM [publicdata:samples.gsod] LIMIT 1000")
     * }}}
     */
    def getTypedRows[T <: HasAnnotation : TypeTag](newSource: String = null)
    : Iterator[T] = {
      val bqt = BigQueryType[T]
      val rows = if (newSource == null) {
        // newSource is missing, T's companion object must have either table or query
        if (bqt.isTable) {
          self.getTableRows(bqt.table.get)
        } else if (bqt.isQuery) {
          self.getQueryRows(bqt.query.get)
        } else {
          throw new IllegalArgumentException(s"Missing table or query field in companion object")
        }
      } else {
        // newSource can be either table or query
        val table = scala.util.Try(BigQueryIO.parseTableSpec(newSource)).toOption
        if (table.isDefined) {
          self.getTableRows(table.get)
        } else {
          self.getQueryRows(newSource)
        }
      }
      rows.map(bqt.fromTableRow)
    }

    /**
     * Write a List of rows to a BigQuery table. Note that element type `T` must be annotated with
     * [[BigQueryType]].
     */
    def writeTypedRows[T <: HasAnnotation : TypeTag]
    (table: TableReference, rows: List[T],
     writeDisposition: WriteDisposition,
     createDisposition: CreateDisposition): Unit = {
      val bqt = BigQueryType[T]
      self.writeTableRows(
        table, rows.map(bqt.toTableRow), bqt.schema,
        writeDisposition, createDisposition)
    }

    /**
     * Write a List of rows to a BigQuery table. Note that element type `T` must be annotated with
     * [[BigQueryType]].
     */
    def writeTypedRows[T <: HasAnnotation : TypeTag]
    (tableSpec: String, rows: List[T],
     writeDisposition: WriteDisposition = WRITE_EMPTY,
     createDisposition: CreateDisposition = CREATE_IF_NEEDED): Unit =
      writeTypedRows(
        BigQueryIO.parseTableSpec(tableSpec), rows,
        writeDisposition, createDisposition)

  }

}
