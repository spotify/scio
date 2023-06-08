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

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.bigquery.BigQueryTyped.Table.{WriteParam => TableWriteParam}
import com.spotify.scio.bigquery.TableRowJsonIO.{WriteParam => TableRowJsonWriteParam}
import com.spotify.scio.bigquery.BigQueryTypedTable.{WriteParam => TypedTableWriteParam}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.{
  coders,
  BigQueryTyped,
  BigQueryTypedTable,
  Clustering,
  Sharding,
  Table,
  TableRow,
  TableRowJsonIO,
  TimePartitioning
}
import com.spotify.scio.coders.Coder
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{
  CreateDisposition,
  Method,
  WriteDisposition
}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import com.spotify.scio.util.FilenamePolicySupplier
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy
import org.joda.time.Duration

/** Enhanced version of [[SCollection]] with BigQuery methods. */
final class SCollectionTableRowOps[T <: TableRow](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection as a BigQuery table. Note that elements must be of type
   * [[com.google.api.services.bigquery.model.TableRow TableRow]].
   */
  def saveAsBigQueryTable(
    table: Table,
    schema: TableSchema = TypedTableWriteParam.DefaultSchema,
    writeDisposition: WriteDisposition = TypedTableWriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = TypedTableWriteParam.DefaultCreateDisposition,
    tableDescription: String = TypedTableWriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = TypedTableWriteParam.DefaultTimePartitioning,
    clustering: Clustering = TypedTableWriteParam.DefaultClustering,
    method: Method = TypedTableWriteParam.DefaultMethod,
    triggeringFrequency: Duration = TypedTableWriteParam.DefaultTriggeringFrequency,
    sharding: Sharding = TypedTableWriteParam.DefaultSharding,
    failedInsertRetryPolicy: InsertRetryPolicy =
      TypedTableWriteParam.DefaultFailedInsertRetryPolicy,
    configOverride: TypedTableWriteParam.ConfigOverride[TableRow] =
      TableWriteParam.defaultConfigOverride
  ): ClosedTap[TableRow] = {
    val param = TypedTableWriteParam(
      method,
      schema,
      writeDisposition,
      createDisposition,
      tableDescription,
      timePartitioning,
      clustering,
      triggeringFrequency,
      sharding,
      failedInsertRetryPolicy,
      configOverride
    )

    self
      .covary[TableRow]
      .write(BigQueryTypedTable(table, Format.TableRow)(coders.tableRowCoder))(param)
  }

  /**
   * Save this SCollection as a BigQuery TableRow JSON text file. Note that elements must be of type
   * [[com.google.api.services.bigquery.model.TableRow TableRow]].
   */
  def saveAsTableRowJsonFile(
    path: String,
    numShards: Int = TableRowJsonWriteParam.DefaultNumShards,
    suffix: String = TableRowJsonWriteParam.DefaultSuffix,
    compression: Compression = TableRowJsonWriteParam.DefaultCompression,
    shardNameTemplate: String = TableRowJsonWriteParam.DefaultShardNameTemplate,
    tempDirectory: String = TableRowJsonWriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier =
      TableRowJsonWriteParam.DefaultFilenamePolicySupplier,
    prefix: String = TableRowJsonWriteParam.DefaultPrefix
  ): ClosedTap[TableRow] = {
    self
      .covary[TableRow]
      .write(TableRowJsonIO(path))(
        TableRowJsonWriteParam(
          suffix = suffix,
          numShards = numShards,
          compression = compression,
          filenamePolicySupplier = filenamePolicySupplier,
          prefix = prefix,
          shardNameTemplate = shardNameTemplate,
          tempDirectory = tempDirectory
        )
      )
  }
}

/** Enhanced version of [[SCollection]] with BigQuery methods */
final class SCollectionGenericRecordOps[T <: GenericRecord](private val self: SCollection[T])
    extends AnyVal {

  /**
   * Save this SCollection as a BigQuery table using Avro writing function. Note that elements must
   * be of type [[org.apache.avro.generic.GenericRecord GenericRecord]].
   */
  def saveAsBigQueryTable(
    table: Table,
    schema: TableSchema = TypedTableWriteParam.DefaultSchema,
    writeDisposition: WriteDisposition = TypedTableWriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = TypedTableWriteParam.DefaultCreateDisposition,
    tableDescription: String = TypedTableWriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = TypedTableWriteParam.DefaultTimePartitioning,
    clustering: Clustering = TypedTableWriteParam.DefaultClustering,
    method: Method = TypedTableWriteParam.DefaultMethod,
    triggeringFrequency: Duration = TypedTableWriteParam.DefaultTriggeringFrequency,
    sharding: Sharding = TypedTableWriteParam.DefaultSharding,
    failedInsertRetryPolicy: InsertRetryPolicy =
      TypedTableWriteParam.DefaultFailedInsertRetryPolicy,
    configOverride: TypedTableWriteParam.ConfigOverride[GenericRecord] =
      TypedTableWriteParam.defaultConfigOverride[GenericRecord]
  ): ClosedTap[GenericRecord] = {
    val param = TypedTableWriteParam(
      method,
      schema,
      writeDisposition,
      createDisposition,
      tableDescription,
      timePartitioning,
      clustering,
      triggeringFrequency,
      sharding,
      failedInsertRetryPolicy,
      configOverride
    )
    self
      .covary[GenericRecord]
      .write(
        BigQueryTypedTable(table, Format.GenericRecord)(
          self.coder.asInstanceOf[Coder[GenericRecord]]
        )
      )(param)
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
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]. For example:
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
    createDisposition: CreateDisposition = TableWriteParam.DefaultCreateDisposition,
    clustering: Clustering = TableWriteParam.DefaultClustering,
    method: Method = TableWriteParam.DefaultMethod,
    triggeringFrequency: Duration = TableWriteParam.DefaultTriggeringFrequency,
    sharding: Sharding = TableWriteParam.DefaultSharding,
    failedInsertRetryPolicy: InsertRetryPolicy = TableWriteParam.DefaultFailedInsertRetryPolicy,
    configOverride: TableWriteParam.ConfigOverride[T] = TableWriteParam.defaultConfigOverride
  )(implicit tt: TypeTag[T], ct: ClassTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = TableWriteParam[T](
      method,
      writeDisposition,
      createDisposition,
      timePartitioning,
      clustering,
      triggeringFrequency,
      sharding,
      failedInsertRetryPolicy,
      configOverride
    )
    self.write(BigQueryTyped.Table[T](table))(param)
  }
}

trait SCollectionSyntax {
  implicit def bigQuerySCollectionTableRowOps[T <: TableRow](
    sc: SCollection[T]
  ): SCollectionTableRowOps[T] =
    new SCollectionTableRowOps[T](sc)

  implicit def bigQuerySCollectionGenericRecordOps[T <: GenericRecord](
    sc: SCollection[T]
  ): SCollectionGenericRecordOps[T] =
    new SCollectionGenericRecordOps[T](sc)

  implicit def bigQuerySCollectionTypedOps[T <: HasAnnotation](
    sc: SCollection[T]
  ): SCollectionTypedOps[T] =
    new SCollectionTypedOps[T](sc)
}
