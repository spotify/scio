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
import com.spotify.scio.bigquery.TableRowJsonIO.{WriteParam => TableRowJsonWriteParam}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io._
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{
  CreateDisposition,
  Method,
  WriteDisposition
}
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy
import org.apache.beam.sdk.transforms.errorhandling.{BadRecord, ErrorHandler}
import org.joda.time.Duration

import scala.reflect.runtime.universe._

/** Enhanced version of [[SCollection]] with BigQuery methods. */
final class SCollectionTableRowOps[T <: TableRow](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection as a BigQuery table. Note that elements must be of type
   * [[com.google.api.services.bigquery.model.TableRow TableRow]].
   *
   * @return
   *   a [[ClosedTap]] containing some side output(s) depending on the given parameters
   *   - [[BigQueryIO.SuccessfulTableLoads]] if `method` is [[Method.FILE_LOADS]].
   *   - [[BigQueryIO.SuccessfulInserts]] if `method` is [[Method.STREAMING_INSERTS]] and
   *     `successfulInsertsPropagation` is `true`.
   *   - [[BigQueryIO.SuccessfulStorageApiInserts]] if `method` id [[Method.STORAGE_WRITE_API]] or
   *     [[Method.STORAGE_API_AT_LEAST_ONCE]].
   *   - [[BigQueryIO.FailedInserts]] if `method` is [[Method.FILE_LOADS]] or
   *     [[Method.STREAMING_INSERTS]].
   *   - [[BigQueryIO.FailedInsertsWithErr]] if `method` is [[Method.STREAMING_INSERTS]] and
   *     `extendedErrorInfo` is `true`.
   *   - [[BigQueryIO.FailedStorageApiInserts]] if `method` id [[Method.STORAGE_WRITE_API]] or
   *     [[Method.STORAGE_API_AT_LEAST_ONCE]].
   */
  def saveAsBigQueryTable(
    table: Table,
    schema: TableSchema = BigQueryIO.WriteParam.DefaultSchema,
    writeDisposition: WriteDisposition = BigQueryIO.WriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = BigQueryIO.WriteParam.DefaultCreateDisposition,
    tableDescription: String = BigQueryIO.WriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = BigQueryIO.WriteParam.DefaultTimePartitioning,
    clustering: Clustering = BigQueryIO.WriteParam.DefaultClustering,
    method: Method = BigQueryIO.WriteParam.DefaultMethod,
    triggeringFrequency: Duration = BigQueryIO.WriteParam.DefaultTriggeringFrequency,
    sharding: Sharding = BigQueryIO.WriteParam.DefaultSharding,
    failedInsertRetryPolicy: InsertRetryPolicy =
      BigQueryIO.WriteParam.DefaultFailedInsertRetryPolicy,
    successfulInsertsPropagation: Boolean =
      BigQueryIO.WriteParam.DefaultSuccessfulInsertsPropagation,
    extendedErrorInfo: Boolean = BigQueryIO.WriteParam.DefaultExtendedErrorInfo,
    errorHandler: ErrorHandler[BadRecord, _] = BigQueryIO.WriteParam.DefaultErrorHandler,
    configOverride: BigQueryIO.WriteParam.ConfigOverride[TableRow] =
      BigQueryIO.WriteParam.DefaultConfigOverride
  ): ClosedTap[TableRow] = {
    val param = BigQueryIO.WriteParam[TableRow](
      BigQueryIO.Format.Default(),
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
      successfulInsertsPropagation,
      extendedErrorInfo,
      errorHandler,
      configOverride
    )

    self
      .covary[TableRow]
      .write(BigQueryIO(table))(param)
  }

  /**
   * Save this SCollection as a BigQuery TableRow JSON text file. Note that elements must be of type
   * [[com.google.api.services.bigquery.model.TableRow TableRow]].
   *
   * @return
   *   a [[ClosedTap]] containing some side output(s) depending on the given parameters
   *   - [[BigQueryIO.SuccessfulTableLoads]] if `method` is [[Method.FILE_LOADS]].
   *   - [[BigQueryIO.SuccessfulInserts]] if `method` is [[Method.STREAMING_INSERTS]] and
   *     `successfulInsertsPropagation` is `true`.
   *   - [[BigQueryIO.SuccessfulStorageApiInserts]] if `method` id [[Method.STORAGE_WRITE_API]] or
   *     [[Method.STORAGE_API_AT_LEAST_ONCE]].
   *   - [[BigQueryIO.FailedInserts]] if `method` is [[Method.FILE_LOADS]] or
   *     [[Method.STREAMING_INSERTS]].
   *   - [[BigQueryIO.FailedInsertsWithErr]] if `method` is [[Method.STREAMING_INSERTS]] and
   *     `extendedErrorInfo` is `true`.
   *   - [[BigQueryIO.FailedStorageApiInserts]] if `method` id [[Method.STORAGE_WRITE_API]] or
   *     [[Method.STORAGE_API_AT_LEAST_ONCE]].
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
   *
   * @return
   *   a [[ClosedTap]] containing some side output(s) depending on the given parameters
   *   - [[BigQueryIO.SuccessfulTableLoads]] if `method` is [[Method.FILE_LOADS]].
   *   - [[BigQueryIO.SuccessfulInserts]] if `method` is [[Method.STREAMING_INSERTS]] and
   *     `successfulInsertsPropagation` is `true`.
   *   - [[BigQueryIO.SuccessfulStorageApiInserts]] if `method` id [[Method.STORAGE_WRITE_API]] or
   *     [[Method.STORAGE_API_AT_LEAST_ONCE]].
   *   - [[BigQueryIO.FailedInserts]] if `method` is [[Method.FILE_LOADS]] or
   *     [[Method.STREAMING_INSERTS]].
   *   - [[BigQueryIO.FailedInsertsWithErr]] if `method` is [[Method.STREAMING_INSERTS]] and
   *     `extendedErrorInfo` is `true`.
   *   - [[BigQueryIO.FailedStorageApiInserts]] if `method` id [[Method.STORAGE_WRITE_API]] or
   *     [[Method.STORAGE_API_AT_LEAST_ONCE]].
   */
  def saveAsBigQueryTable(
    table: Table,
    schema: TableSchema = BigQueryIO.WriteParam.DefaultSchema,
    writeDisposition: WriteDisposition = BigQueryIO.WriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = BigQueryIO.WriteParam.DefaultCreateDisposition,
    tableDescription: String = BigQueryIO.WriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = BigQueryIO.WriteParam.DefaultTimePartitioning,
    clustering: Clustering = BigQueryIO.WriteParam.DefaultClustering,
    method: Method = BigQueryIO.WriteParam.DefaultMethod,
    triggeringFrequency: Duration = BigQueryIO.WriteParam.DefaultTriggeringFrequency,
    sharding: Sharding = BigQueryIO.WriteParam.DefaultSharding,
    failedInsertRetryPolicy: InsertRetryPolicy =
      BigQueryIO.WriteParam.DefaultFailedInsertRetryPolicy,
    successfulInsertsPropagation: Boolean =
      BigQueryIO.WriteParam.DefaultSuccessfulInsertsPropagation,
    extendedErrorInfo: Boolean = BigQueryIO.WriteParam.DefaultExtendedErrorInfo,
    errorHandler: ErrorHandler[BadRecord, _] = BigQueryIO.WriteParam.DefaultErrorHandler,
    configOverride: BigQueryIO.WriteParam.ConfigOverride[GenericRecord] =
      BigQueryIO.WriteParam.DefaultConfigOverride
  ): ClosedTap[GenericRecord] = {
    val param = BigQueryIO.WriteParam[GenericRecord](
      BigQueryIO.Format.Avro(),
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
      successfulInsertsPropagation,
      extendedErrorInfo,
      errorHandler,
      configOverride
    )
    self
      .covary[GenericRecord]
      .write(BigQueryIO[GenericRecord](table)(self.coder.asInstanceOf[Coder[GenericRecord]]))(param)
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
   * p.saveAsTypedBigQueryTable(Table("myproject:mydataset.mytable"))
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
   *   .saveAsTypedBigQueryTable(Table("myproject:samples.gsod"))
   * }}}
   *
   * *
   * @return
   *   a [[ClosedTap]] containing some side output(s) depending on the given parameters
   *   - [[BigQueryIO.SuccessfulTableLoads]] if `method` is [[Method.FILE_LOADS]].
   *   - [[BigQueryIO.SuccessfulInserts]] if `method` is [[Method.STREAMING_INSERTS]] and
   *     `successfulInsertsPropagation` is `true`.
   *   - [[BigQueryIO.SuccessfulStorageApiInserts]] if `method` id [[Method.STORAGE_WRITE_API]] or
   *     [[Method.STORAGE_API_AT_LEAST_ONCE]].
   *   - [[BigQueryIO.FailedInserts]] if `method` is [[Method.STREAMING_INSERTS]].
   *   - [[BigQueryIO.FailedInsertsWithErr]] if `method` is [[Method.STREAMING_INSERTS]] and
   *     `extendedErrorInfo` is `true`.
   *   - [[BigQueryIO.FailedStorageApiInserts]] if `method` id [[Method.STORAGE_WRITE_API]] or
   *     [[Method.STORAGE_API_AT_LEAST_ONCE]].
   */
  def saveAsTypedBigQueryTable(
    table: Table,
    timePartitioning: TimePartitioning = BigQueryIO.WriteParam.DefaultTimePartitioning,
    writeDisposition: WriteDisposition = BigQueryIO.WriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = BigQueryIO.WriteParam.DefaultCreateDisposition,
    clustering: Clustering = BigQueryIO.WriteParam.DefaultClustering,
    method: Method = BigQueryIO.WriteParam.DefaultMethod,
    triggeringFrequency: Duration = BigQueryIO.WriteParam.DefaultTriggeringFrequency,
    sharding: Sharding = BigQueryIO.WriteParam.DefaultSharding,
    failedInsertRetryPolicy: InsertRetryPolicy =
      BigQueryIO.WriteParam.DefaultFailedInsertRetryPolicy,
    successfulInsertsPropagation: Boolean =
      BigQueryIO.WriteParam.DefaultSuccessfulInsertsPropagation,
    extendedErrorInfo: Boolean = BigQueryIO.WriteParam.DefaultExtendedErrorInfo,
    errorHandler: ErrorHandler[BadRecord, _] = BigQueryIO.WriteParam.DefaultErrorHandler,
    configOverride: BigQueryIO.WriteParam.ConfigOverride[T] =
      BigQueryIO.WriteParam.DefaultConfigOverride
  )(implicit tt: TypeTag[T], coder: Coder[T]): ClosedTap[T] = {
    val bqt = BigQueryType[T]
    val param = BigQueryIO.WriteParam[T](
      BigQueryIO.Format.Avro[T](bqt),
      method,
      bqt.schema,
      writeDisposition,
      createDisposition,
      BigQueryIO.WriteParam.DefaultTableDescription,
      timePartitioning,
      clustering,
      triggeringFrequency,
      sharding,
      failedInsertRetryPolicy,
      successfulInsertsPropagation,
      extendedErrorInfo,
      errorHandler,
      configOverride
    )
    self.write(BigQueryIO[T](table))(param)
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
