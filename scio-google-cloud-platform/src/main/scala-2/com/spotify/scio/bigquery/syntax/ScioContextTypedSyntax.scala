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
import com.spotify.scio.bigquery.{
  BigQueryStorage,
  BigQueryType,
  BigQueryTyped,
  Query,
  Source,
  Table
}
import com.spotify.scio.coders.Coder
import com.spotify.scio.schemas.Schema
import com.spotify.scio.values._
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[syntax] trait ScioContextTypedBaseOps {
  private[syntax] val self: ScioContext

  def typedBigQuery[T <: HasAnnotation: ClassTag: TypeTag: Coder](): SCollection[T] =
    typedBigQuery(None)

  def typedBigQuery[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    newSource: Source
  ): SCollection[T] = typedBigQuery(Option(newSource))

  /** Get a typed SCollection for BigQuery Table or a SELECT query using the Storage API. */
  def typedBigQuery[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    newSource: Option[Source]
  ): SCollection[T] = {
    val bqt = BigQueryType[T]
    if (bqt.isStorage) {
      newSource
        .asInstanceOf[Option[Table]]
        .map(typedBigQueryStorage(_))
        .getOrElse(typedBigQueryStorage())
    } else {
      self.read(BigQueryTyped.dynamic[T](newSource))
    }
  }

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def typedBigQueryTable[T: Schema: Coder: ClassTag](table: Table): SCollection[T] =
    self.read(BigQueryTyped.BeamSchema(table))

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def typedBigQueryTable[T: Schema: Coder: ClassTag](
    table: Table,
    parseFn: SchemaAndRecord => T
  ): SCollection[T] =
    self.read(BigQueryTyped.BeamSchema(table, parseFn))

  /**
   * Get a typed SCollection for a BigQuery storage API.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromSchema BigQueryType.fromStorage]] or
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]
   */
  def typedBigQueryStorage[T <: HasAnnotation: ClassTag: TypeTag: Coder](): SCollection[T] = {
    val bqt = BigQueryType[T]
    if (bqt.isQuery) {
      self.read(BigQueryTyped.StorageQuery[T](Query(bqt.query.get)))
    } else {
      val table = Table.Spec(bqt.table.get)
      val rr = bqt.rowRestriction
      val fields = bqt.selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields)
      self.read(BigQueryTyped.Storage[T](table, fields, rr))
    }
  }

  def typedBigQueryStorage[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    table: Table
  ): SCollection[T] =
    self.read(
      BigQueryTyped.Storage[T](
        table,
        BigQueryType[T].selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields),
        BigQueryType[T].rowRestriction
      )
    )

  def typedBigQueryStorage[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    rowRestriction: String
  ): SCollection[T] = {
    val bqt = BigQueryType[T]
    val table = Table.Spec(bqt.table.get)
    self.read(
      BigQueryTyped.Storage[T](
        table,
        bqt.selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields),
        Option(rowRestriction)
      )
    )
  }

  def typedBigQueryStorage[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    table: Table,
    rowRestriction: String
  ): SCollection[T] =
    self.read(
      BigQueryTyped.Storage[T](
        table,
        BigQueryType[T].selectedFields.getOrElse(BigQueryStorage.ReadParam.DefaultSelectFields),
        Option(rowRestriction)
      )
    )

  def typedBigQueryStorage[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    table: Table,
    selectedFields: List[String],
    rowRestriction: String
  ): SCollection[T] =
    self.read(
      BigQueryTyped.Storage[T](
        table,
        selectedFields,
        Option(rowRestriction)
      )
    )
}

final class ScioContextTypedOps(private[syntax] val self: ScioContext)
    extends ScioContextTypedBaseOps

trait ScioContextTypedSyntax {
  implicit def bigQueryScioContextTypedOps(sc: ScioContext): ScioContextTypedOps =
    new ScioContextTypedOps(sc)
}
