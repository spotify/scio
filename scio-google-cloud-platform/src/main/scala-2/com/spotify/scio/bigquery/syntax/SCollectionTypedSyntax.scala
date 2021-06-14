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

import com.spotify.scio.bigquery.BigQueryTyped.Table.{WriteParam => TableWriteParam}
import com.spotify.scio.bigquery.BigQueryTyped.BeamSchema.{WriteParam => TypedWriteParam}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.{BigQueryTyped, TimePartitioning}
import com.spotify.scio.coders.Coder
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.spotify.scio.bigquery.Table
import com.spotify.scio.schemas.Schema

/** Enhanced version of [[SCollection]] with BigQuery methods. */
final class SCollectionBeamSchemaOps[T: ClassTag](private val self: SCollection[T]) {
  def saveAsBigQueryTable(
    table: Table,
    writeDisposition: WriteDisposition = TypedWriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = TypedWriteParam.DefaultCreateDisposition,
    tableDescription: String = TypedWriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = TypedWriteParam.DefaultTimePartitioning
  )(implicit schema: Schema[T], coder: Coder[T]): ClosedTap[T] = {
    val param =
      TypedWriteParam(
        writeDisposition,
        createDisposition,
        tableDescription,
        timePartitioning
      )
    self
      .write(BigQueryTyped.BeamSchema(table))(param)
      .asInstanceOf[ClosedTap[T]]
  }
}

/** Enhanced version of [[SCollection]] with BigQuery methods. */
final class SCollectionTypedOps[T <: HasAnnotation](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as a BigQuery table. Note that element type `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType BigQueryType]].
   *
   * This could be a complete case class with
   * [[com.spotify.scio.bigquery.types.BigQueryType.toTable BigQueryType.toTable]]. For example:
   *
   * {{{
   * @BigQueryType.toTable
   * case class Result(name: String, score: Double)
   *
   * val p: SCollection[Result] = // process data and convert elements to Result
   * p.saveAsTypedBigQueryTable(Table.Spec("myproject:mydataset.mytable"))
   * }}}
   *
   * It could also be an empty class with schema from
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromSchema]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]], or
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]. For
   * example:
   *
   * {{{
   * @BigQueryType.fromTable("bigquery-public-data:samples.gsod")
   * class Row
   *
   * sc.typedBigQuery[Row]()
   *   .sample(withReplacement = false, fraction = 0.1)
   *   .saveAsTypedBigQueryTable(Table.Spec("myproject:samples.gsod"))
   * }}}
   */
  def saveAsTypedBigQueryTable(
    table: Table,
    timePartitioning: TimePartitioning = TableWriteParam.DefaultTimePartitioning,
    writeDisposition: WriteDisposition = TableWriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = TableWriteParam.DefaultCreateDisposition
  )(implicit tt: TypeTag[T], ct: ClassTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = TableWriteParam(writeDisposition, createDisposition, timePartitioning)
    self.write(BigQueryTyped.Table[T](table))(param)
  }
}

trait SCollectionTypedSyntax {
  implicit def bigQuerySCollectionTypedOps[T <: HasAnnotation](
    sc: SCollection[T]
  ): SCollectionTypedOps[T] =
    new SCollectionTypedOps[T](sc)

  implicit def bigQuerySCollectionBeamSchemaOps[T: ClassTag](
    sc: SCollection[T]
  ): SCollectionBeamSchemaOps[T] =
    new SCollectionBeamSchemaOps[T](sc)
}
