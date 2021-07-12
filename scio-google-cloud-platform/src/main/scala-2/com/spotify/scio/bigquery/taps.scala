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

import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions
import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{Tap, Taps}

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

final case class BigQueryTypedTaps(self: Taps) {
  import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
  import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
  import self.mkTap

  private lazy val bqc = BigQuery.defaultInstance()

  private lazy val baseTaps = new BigQueryTaps(self)

  /** Get a `Future[Tap[T]]` for typed BigQuery source. */
  def typedBigQuery[T <: HasAnnotation: TypeTag: ClassTag: Coder](
    newSource: String = null
  ): Future[Tap[T]] = {
    val bqt = BigQueryType[T]
    lazy val table =
      scala.util.Try(BigQueryHelpers.parseTableSpec(newSource)).toOption
    val rows =
      newSource match {
        // newSource is missing, T's companion object must have either table or query
        case null if bqt.isTable =>
          baseTaps.bigQueryTable(bqt.table.get)
        case null if bqt.isQuery =>
          baseTaps.bigQuerySelect(bqt.query.get)
        case null =>
          throw new IllegalArgumentException(s"Missing table or query field in companion object")
        case _ if table.isDefined =>
          baseTaps.bigQueryTable(table.get)
        case _ =>
          baseTaps.bigQuerySelect(newSource)
      }
    import scala.concurrent.ExecutionContext.Implicits.global
    rows.map(_.map(bqt.fromTableRow))
  }

  def typedBigQueryStorage[T: TypeTag: Coder](
    table: TableReference,
    readOptions: TableReadOptions
  ): Future[Tap[T]] = {
    val fn = BigQueryType[T].fromTableRow
    mkTap(
      s"BigQuery direct read table: $table",
      () => bqc.tables.exists(table),
      () => {
        val selectedFields = readOptions.getSelectedFieldsList.asScala.toList
        val rowRestriction = Option(readOptions.getRowRestriction)
        BigQueryStorage(Table.Ref(table), selectedFields, rowRestriction)
          .tap()
          .map(fn)
      }
    )
  }
}

object BigQueryTypedTaps {
  implicit def toBigQueryTypedTaps(underlying: Taps): BigQueryTypedTaps =
    BigQueryTypedTaps(underlying)
}
