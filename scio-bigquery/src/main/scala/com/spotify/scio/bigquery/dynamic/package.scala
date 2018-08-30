/*
 * Copyright 2017 Spotify AB.
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

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.io.Tap
import com.spotify.scio.util.Functions
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.bigquery.{DynamicDestinations, TableDestination}
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.values.ValueInSingleWindow

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * BigQuery package for dynamic table destinations. Import All.
 *
 * {{{
 * import com.spotify.scio.bigquery.dynamic._
 * }}}
 */
package object dynamic {

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic
   * destinations methods.
   */
  implicit class DynamicBigQuerySCollection[T](val self: SCollection[T]) extends AnyVal {

    /**
     * Save this SCollection to dynamic BigQuery tables using the table and schema specified by the
     * [[org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations DynamicDestinations]].
     */
    def saveAsBigQuery(destinations: DynamicDestinations[T, _],
                       formatFn: T => TableRow,
                       writeDisposition: WriteDisposition,
                       createDisposition: CreateDisposition): Future[Tap[T]] = {
      if (self.context.isTest) {
          throw new NotImplementedError(
            "BigQuery with dynamic destinations cannot be used in a test context")
      } else {
        var transform = beam.BigQueryIO.write()
          .to(destinations)
          .withFormatFunction(Functions.serializableFn(formatFn))
        if (createDisposition != null) {
          transform = transform.withCreateDisposition(createDisposition)
        }
        if (writeDisposition != null) {
          transform = transform.withWriteDisposition(writeDisposition)
        }
        self.applyInternal(transform)
      }

      Future.failed(
        new NotImplementedError("BigQuery future with dynamic destinations not implemented"))
    }

    /**
     * Save this SCollection to dynamic BigQuery tables using the specified table function.
     * Note that elements must be of type
     * [[com.google.api.services.bigquery.model.TableRow TableRow]].
     */
    def saveAsBigQuery(schema: TableSchema,
                       writeDisposition: WriteDisposition = null,
                       createDisposition: CreateDisposition = null)
                      (tableFn: ValueInSingleWindow[T] => TableDestination)
                      (implicit ev: T <:< TableRow): Future[Tap[TableRow]] =
      saveAsBigQuery(
        DynamicDestinationsUtil.tableFn(tableFn, schema),
        (t: T) => t.asInstanceOf[TableRow],
        writeDisposition,
        createDisposition)
        .asInstanceOf[Future[Tap[TableRow]]]


    /**
     * Save this SCollection to dynamic BigQuery tables using the specified table function.
     * Note that element type `T` must be annotated with
     * [[com.spotify.scio.bigquery.types.BigQueryType BigQueryType]].
     */
    def saveAsTypedBigQuery(writeDisposition: WriteDisposition = null,
                            createDisposition: CreateDisposition = null)
                           (tableFn: ValueInSingleWindow[T] => TableDestination)
                           (implicit ct: ClassTag[T], tt: TypeTag[T], ev: T <:< HasAnnotation)
    : Future[Tap[T]] = {
      val bqt = BigQueryType[T]
      val destinations = DynamicDestinationsUtil.tableFn(tableFn, bqt.schema)

      saveAsBigQuery(
        destinations,
        bqt.toTableRow,
        writeDisposition,
        createDisposition)
    }

  }

}
