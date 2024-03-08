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

package com.spotify.scio.bigquery.dynamic.syntax

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.bigquery.dynamic._
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.{TableRow, Writes}
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.util.Functions
import com.spotify.scio.values.SCollection
import magnolify.bigquery.TableRowType
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{
  CreateDisposition,
  Method,
  WriteDisposition
}
import org.apache.beam.sdk.io.gcp.bigquery.{DynamicDestinations, TableDestination}
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.values.ValueInSingleWindow

import scala.reflect.runtime.universe._
import scala.util.chaining._

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic destinations
 * methods.
 */
final class DynamicBigQueryOps[T](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection to dynamic BigQuery tables specified by `tableFn`, converting elements of
   * type `T` to `TableRow` via the implicitly-available `TableRowType[T]`
   */
  def saveAsBigQuery(
    writeDisposition: WriteDisposition = null,
    createDisposition: CreateDisposition = null,
    extendedErrorInfo: Boolean = false
  )(
    tableFn: ValueInSingleWindow[T] => TableDestination
  )(implicit tableRowType: TableRowType[T]): ClosedTap[Nothing] = {
    val destinations = DynamicDestinationsUtil.tableFn(tableFn, tableRowType.schema)

    new DynamicBigQueryOps(self).saveAsBigQuery(
      destinations,
      tableRowType.to,
      writeDisposition,
      createDisposition,
      false,
      extendedErrorInfo
    )
  }

  /**
   * Save this SCollection to dynamic BigQuery tables using the table and schema specified by the
   * [[org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations DynamicDestinations]].
   */
  def saveAsBigQuery(
    destinations: DynamicDestinations[T, _],
    formatFn: T => TableRow,
    writeDisposition: WriteDisposition,
    createDisposition: CreateDisposition,
    successfulInsertsPropagation: Boolean,
    extendedErrorInfo: Boolean
  ): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "BigQuery with dynamic destinations cannot be used in a test context"
      )
    }

    val method = Method.DEFAULT
    val t = beam.BigQueryIO
      .write()
      .to(destinations)
      .withFormatFunction(Functions.serializableFn(formatFn))
      .pipe(w => Option(createDisposition).fold(w)(w.withCreateDisposition))
      .pipe(w => Option(writeDisposition).fold(w)(w.withWriteDisposition))
      .pipe(w => Writes.withSuccessfulInsertsPropagation(method, w)(successfulInsertsPropagation))
      .pipe(w => if (extendedErrorInfo) w.withExtendedErrorInfo() else w)

    val wr = self.applyInternal(t)
    val outputs =
      Writes.sideOutputs(self, method, successfulInsertsPropagation, extendedErrorInfo, wr)
    ClosedTap[Nothing](EmptyTap, Some(outputs))
  }
}

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic destinations
 * methods.
 */
final class DynamicTableRowBigQueryOps[T <: TableRow](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection to dynamic BigQuery tables using the specified table function. Note that
   * elements must be of type [[com.google.api.services.bigquery.model.TableRow TableRow]].
   */
  def saveAsBigQuery(
    schema: TableSchema,
    writeDisposition: WriteDisposition = null,
    createDisposition: CreateDisposition = null,
    extendedErrorInfo: Boolean = false
  )(tableFn: ValueInSingleWindow[T] => TableDestination): ClosedTap[Nothing] =
    new DynamicBigQueryOps(self).saveAsBigQuery(
      DynamicDestinationsUtil.tableFn(tableFn, schema),
      identity,
      writeDisposition,
      createDisposition,
      false,
      extendedErrorInfo
    )
}

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic destinations
 * methods.
 */
final class DynamicTypedBigQueryOps[T <: HasAnnotation](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection to dynamic BigQuery tables using the specified table function. Note that
   * element type `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType BigQueryType]].
   */
  def saveAsTypedBigQuery(
    writeDisposition: WriteDisposition = null,
    createDisposition: CreateDisposition = null,
    extendedErrorInfo: Boolean = false
  )(
    tableFn: ValueInSingleWindow[T] => TableDestination
  )(implicit tt: TypeTag[T]): ClosedTap[Nothing] = {
    val bqt = BigQueryType[T]
    val destinations = DynamicDestinationsUtil.tableFn(tableFn, bqt.schema)

    new DynamicBigQueryOps(self).saveAsBigQuery(
      destinations,
      bqt.toTableRow,
      writeDisposition,
      createDisposition,
      false,
      extendedErrorInfo
    )
  }
}

trait SCollectionSyntax {
  implicit def bigQueryDynamicOps[T](sc: SCollection[T]): DynamicBigQueryOps[T] =
    new DynamicBigQueryOps[T](sc)

  implicit def bigQueryTableRowDynamicOps[T <: TableRow](
    sc: SCollection[T]
  ): DynamicTableRowBigQueryOps[T] =
    new DynamicTableRowBigQueryOps[T](sc)

  implicit def bigQueryTypedDynamicOps[T <: HasAnnotation](
    sc: SCollection[T]
  ): DynamicTypedBigQueryOps[T] =
    new DynamicTypedBigQueryOps[T](sc)
}
