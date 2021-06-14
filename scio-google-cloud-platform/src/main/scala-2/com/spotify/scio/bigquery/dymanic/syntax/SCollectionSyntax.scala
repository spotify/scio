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

import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.dynamic._
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination
import org.apache.beam.sdk.values.ValueInSingleWindow

import scala.reflect.runtime.universe._

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic
 * destinations methods.
 */
final class DynamicTypedBigQueryOps[T <: HasAnnotation](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection to dynamic BigQuery tables using the specified table function.
   * Note that element type `T` must be annotated with
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

trait SCollectionTypedSyntax {
  implicit def bigQueryTypedDynamicOps[T <: HasAnnotation](
    sc: SCollection[T]
  ): DynamicTypedBigQueryOps[T] =
    new DynamicTypedBigQueryOps[T](sc)
}
