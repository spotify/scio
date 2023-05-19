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
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.bigquery.ExtendedErrorInfo._
import com.spotify.scio.bigquery.dynamic._
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.util.Functions
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.bigquery.{DynamicDestinations, TableDestination}
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.values.ValueInSingleWindow
import com.spotify.scio.bigquery.ExtendedErrorInfo

import scala.reflect.runtime.universe._

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic destinations
 * methods.
 */
final class DynamicBigQueryOps[T](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection to dynamic BigQuery tables using the table and schema specified by the
   * [[org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations DynamicDestinations]].
   */
  def saveAsBigQuery(
    destinations: DynamicDestinations[T, _],
    formatFn: T => TableRow,
    writeDisposition: WriteDisposition,
    createDisposition: CreateDisposition
  ): ClosedTap[Nothing] =
    saveAsBigQuery(
      destinations,
      formatFn,
      writeDisposition,
      createDisposition,
      ExtendedErrorInfo.Disabled
    )(_ => ())

  /**
   * Save this SCollection to dynamic BigQuery tables using the table and schema specified by the
   * [[org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations DynamicDestinations]].
   */
  def saveAsBigQuery[Info](
    destinations: DynamicDestinations[T, _],
    formatFn: T => TableRow,
    writeDisposition: WriteDisposition,
    createDisposition: CreateDisposition,
    extendedErrorInfo: ExtendedErrorInfo[Info]
  )(
    insertErrorTransform: SCollection[Info] => Unit
  ): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "BigQuery with dynamic destinations cannot be used in a test context"
      )
    } else {
      var transform = beam.BigQueryIO
        .write()
        .to(destinations)
        .withFormatFunction(Functions.serializableFn(formatFn))

      if (createDisposition != null) {
        transform = transform.withCreateDisposition(createDisposition)
      }
      if (writeDisposition != null) {
        transform = transform.withWriteDisposition(writeDisposition)
      }

      transform = extendedErrorInfo match {
        case Disabled => transform
        case Enabled  => transform.withExtendedErrorInfo()
      }

      val wr = self.applyInternal(transform)

      insertErrorTransform(extendedErrorInfo.coll(self.context, wr))
    }

    ClosedTap[Nothing](EmptyTap)
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
    createDisposition: CreateDisposition = null
  )(tableFn: ValueInSingleWindow[T] => TableDestination): ClosedTap[Nothing] =
    new DynamicBigQueryOps(self).saveAsBigQuery(
      DynamicDestinationsUtil.tableFn(tableFn, schema),
      identity,
      writeDisposition,
      createDisposition
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
    createDisposition: CreateDisposition = null
  )(
    tableFn: ValueInSingleWindow[T] => TableDestination
  )(implicit tt: TypeTag[T]): ClosedTap[Nothing] = {
    val bqt = BigQueryType[T]
    val destinations = DynamicDestinationsUtil.tableFn(tableFn, bqt.schema)

    new DynamicBigQueryOps(self).saveAsBigQuery(
      destinations,
      bqt.toTableRow,
      writeDisposition,
      createDisposition
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
