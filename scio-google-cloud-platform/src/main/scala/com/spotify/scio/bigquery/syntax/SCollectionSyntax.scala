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
import com.spotify.scio.bigquery.BigQueryTypedTable.{Format, WriteParam => TypedTableWriteParam}
import com.spotify.scio.bigquery.TableRowJsonIO.{WriteParam => TableRowJsonWriteParam}
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
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
    successfulInsertsPropagation: Boolean = TableWriteParam.DefaultSuccessfulInsertsPropagation,
    extendedErrorInfo: Boolean = TypedTableWriteParam.DefaultExtendedErrorInfo,
    configOverride: TypedTableWriteParam.ConfigOverride[TableRow] =
      TableWriteParam.DefaultConfigOverride
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
      successfulInsertsPropagation,
      extendedErrorInfo,
      configOverride
    )

    self
      .covary[TableRow]
      .write(BigQueryTypedTable(table, Format.TableRow)(coders.tableRowCoder))(param)
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
    successfulInsertsPropagation: Boolean = TableWriteParam.DefaultSuccessfulInsertsPropagation,
    extendedErrorInfo: Boolean = TypedTableWriteParam.DefaultExtendedErrorInfo,
    configOverride: TypedTableWriteParam.ConfigOverride[GenericRecord] =
      TypedTableWriteParam.DefaultConfigOverride
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
      successfulInsertsPropagation,
      extendedErrorInfo,
      configOverride
    )
    if (
      BigQueryUtil
        .isStorageApiWrite(method) && Option(schema).exists(BigQueryUtil.containsType(_, "TIME"))
    ) {
      // Todo remove once https://github.com/apache/beam/issues/34038 is fixed
      throw new IllegalArgumentException(
        "TIME schemas are not currently supported for GenericRecord Storage Write API writes. Please use Write method FILE_LOADS instead, or write TableRows directly."
      )
    }

    self
      .covary[GenericRecord]
      .write(
        BigQueryTypedTable(table, Format.GenericRecordWithLogicalTypes)(
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
    timePartitioning: TimePartitioning = TableWriteParam.DefaultTimePartitioning,
    writeDisposition: WriteDisposition = TableWriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = TableWriteParam.DefaultCreateDisposition,
    clustering: Clustering = TableWriteParam.DefaultClustering,
    method: Method = TableWriteParam.DefaultMethod,
    triggeringFrequency: Duration = TableWriteParam.DefaultTriggeringFrequency,
    sharding: Sharding = TableWriteParam.DefaultSharding,
    failedInsertRetryPolicy: InsertRetryPolicy = TableWriteParam.DefaultFailedInsertRetryPolicy,
    successfulInsertsPropagation: Boolean = TableWriteParam.DefaultSuccessfulInsertsPropagation,
    extendedErrorInfo: Boolean = TableWriteParam.DefaultExtendedErrorInfo,
    configOverride: TableWriteParam.ConfigOverride[T] = TableWriteParam.DefaultConfigOverride,
    format: Format[_] = TableWriteParam.DefaultFormat
  )(implicit tt: TypeTag[T], coder: Coder[T]): ClosedTap[T] = {
    val bqt = BigQueryType[T]

    if (
      format == Format.TableRow && method == Method.FILE_LOADS && BigQueryUtil.containsType(
        bqt.schema,
        "JSON"
      )
    ) {
      throw new IllegalArgumentException(
        "JSON schemas are not supported for typed BigQuery writes using the FILE_LOADS API and TableRow representation. Please either use the STORAGE_WRITE_API method or GenericRecord Format."
      )
    }

    self.write(BigQueryTyped.Table[T](table, format))(
      TableWriteParam(
        method,
        writeDisposition,
        createDisposition,
        timePartitioning,
        clustering,
        triggeringFrequency,
        sharding,
        failedInsertRetryPolicy,
        successfulInsertsPropagation,
        extendedErrorInfo,
        configOverride
      )
    )
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
