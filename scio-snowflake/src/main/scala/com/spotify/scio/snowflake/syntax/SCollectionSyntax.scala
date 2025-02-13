/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.snowflake.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.snowflake.{SnowflakeConnectionOptions, SnowflakeIO, SnowflakeTable}
import com.spotify.scio.values.SCollection
import kantan.csv.RowCodec
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema
import org.apache.beam.sdk.io.snowflake.enums.{CreateDisposition, WriteDisposition}
import org.joda.time.Duration

/** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Snowflake methods. */
final class SnowflakeSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection as a Snowflake database table. The [[SCollection]] is written to CSV
   * files in a bucket, using a provided [[kantan.csv.RowEncoder]] to encode each element as a CSV
   * row. The bucket is then COPYied to the Snowflake table.
   *
   * @see
   *   ''Reading from Snowflake'' in the
   *   [[https://beam.apache.org/documentation/io/built-in/snowflake/ Beam `SnowflakeIO` documentation]]
   * @param connectionOptions
   *   options for configuring a Snowflake integration
   * @param table
   *   table name to be written in Snowflake
   * @param tableSchema
   *   table schema to be used during creating table
   * @param createDisposition
   *   disposition to be used during table preparation
   * @param writeDisposition
   *   disposition to be used during writing to table phase
   * @param snowPipe
   *   name of created
   *   [[https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro SnowPipe]] in Snowflake
   *   dashboard
   * @param shardNumber
   *   number of shards that are created per window
   * @param flushRowLimit
   *   number of row limit that will be saved to the staged file and then loaded to Snowflake
   * @param flushTimeLimit
   *   duration how often staged files will be created and then how often ingested by Snowflake
   *   during streaming
   * @param storageIntegrationName
   *   Storage Integration in Snowflake to be used
   * @param stagingBucketName
   *   cloud bucket (GCS by now) to use as tmp location of CSVs during COPY statement.
   * @param quotationMark
   *   Snowflake-specific quotations around strings
   * @return
   *   [[SCollection]] containing the table elements as parsed from the CSV bucket copied from
   *   Snowflake table
   */
  def saveAsSnowflake(
    connectionOptions: SnowflakeConnectionOptions,
    table: String,
    tableSchema: SnowflakeTableSchema = SnowflakeIO.WriteParam.DefaultTableSchema,
    createDisposition: CreateDisposition = SnowflakeIO.WriteParam.DefaultCreateDisposition,
    writeDisposition: WriteDisposition = SnowflakeIO.WriteParam.DefaultWriteDisposition,
    snowPipe: String = SnowflakeIO.WriteParam.DefaultSnowPipe,
    shardNumber: Integer = SnowflakeIO.WriteParam.DefaultShardNumber,
    flushRowLimit: Integer = SnowflakeIO.WriteParam.DefaultFlushRowLimit,
    flushTimeLimit: Duration = SnowflakeIO.WriteParam.DefaultFlushTimeLimit,
    storageIntegrationName: String = SnowflakeIO.WriteParam.DefaultStorageIntegrationName,
    stagingBucketName: String = SnowflakeIO.WriteParam.DefaultStagingBucketName,
    quotationMark: String = SnowflakeIO.WriteParam.DefaultQuotationMark,
    configOverride: SnowflakeIO.WriteParam.ConfigOverride[T] =
      SnowflakeIO.WriteParam.DefaultConfigOverride
  )(implicit rowCodec: RowCodec[T], coder: Coder[T]): ClosedTap[Nothing] = {
    val param = SnowflakeIO.WriteParam(
      tableSchema = tableSchema,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      snowPipe = snowPipe,
      shardNumber = shardNumber,
      flushRowLimit = flushRowLimit,
      flushTimeLimit = flushTimeLimit,
      storageIntegrationName = storageIntegrationName,
      stagingBucketName = stagingBucketName,
      quotationMark = quotationMark,
      configOverride = configOverride
    )
    self.write(SnowflakeTable[T](connectionOptions, table))(param)
  }
}

trait SCollectionSyntax {
  implicit def snowflakeSCollectionOps[T](sc: SCollection[T]): SnowflakeSCollectionOps[T] =
    new SnowflakeSCollectionOps(sc)
}
