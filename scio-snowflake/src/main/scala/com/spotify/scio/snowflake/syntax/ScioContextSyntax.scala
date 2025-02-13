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

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.snowflake.{
  SnowflakeConnectionOptions,
  SnowflakeIO,
  SnowflakeSelect,
  SnowflakeTable
}
import com.spotify.scio.values.SCollection
import kantan.csv.{RowCodec, RowDecoder}

/** Enhanced version of [[ScioContext]] with Snowflake methods. */
final class SnowflakeScioContextOps(private val self: ScioContext) extends AnyVal {

  /**
   * Get an SCollection for a Snowflake SQL query
   *
   * @param connectionOptions
   *   options for configuring a Snowflake connexion
   * @param query
   *   Snowflake SQL select query
   * @param storageIntegrationName
   *   Storage Integration in Snowflake to be used
   * @param stagingBucketName
   *   cloud bucket (GCS by now) to use as tmp location of CSVs during COPY statement.
   * @param quotationMark
   *   Snowflake-specific quotations around strings
   */
  def snowflakeQuery[T](
    connectionOptions: SnowflakeConnectionOptions,
    query: String,
    storageIntegrationName: String,
    stagingBucketName: String = SnowflakeIO.ReadParam.DefaultStagingBucketName,
    quotationMark: String = SnowflakeIO.ReadParam.DefaultQuotationMark,
    configOverride: SnowflakeIO.ReadParam.ConfigOverride[T] =
      SnowflakeIO.ReadParam.DefaultConfigOverride
  )(implicit rowDecoder: RowDecoder[T], coder: Coder[T]): SCollection[T] = {
    val param = SnowflakeIO.ReadParam(
      storageIntegrationName = storageIntegrationName,
      stagingBucketName = stagingBucketName,
      quotationMark = quotationMark,
      configOverride = configOverride
    )
    self.read(SnowflakeSelect(connectionOptions, query))(param)
  }

  /**
   * Get an SCollection for a Snowflake table
   *
   * @param connectionOptions
   *   options for configuring a Snowflake connexion
   * @param table
   *   Snowflake table
   * @param storageIntegrationName
   *   Storage Integration in Snowflake to be used
   * @param stagingBucketName
   *   cloud bucket (GCS by now) to use as tmp location of CSVs during COPY statement.
   * @param quotationMark
   *   Snowflake-specific quotations around strings
   */
  def snowflakeTable[T](
    connectionOptions: SnowflakeConnectionOptions,
    table: String,
    storageIntegrationName: String,
    stagingBucketName: String = SnowflakeIO.ReadParam.DefaultStagingBucketName,
    quotationMark: String = SnowflakeIO.ReadParam.DefaultQuotationMark,
    configOverride: SnowflakeIO.ReadParam.ConfigOverride[T] =
      SnowflakeIO.ReadParam.DefaultConfigOverride
  )(implicit rowDecoder: RowDecoder[T], coder: Coder[T]): SCollection[T] = {
    // create a read only codec
    implicit val codec: RowCodec[T] = RowCodec.from(rowDecoder, null)
    val param = SnowflakeIO.ReadParam(
      storageIntegrationName = storageIntegrationName,
      stagingBucketName = stagingBucketName,
      quotationMark = quotationMark,
      configOverride = configOverride
    )
    self.read(SnowflakeTable(connectionOptions, table))(param)
  }

}
trait ScioContextSyntax {
  implicit def snowflakeScioContextOps(sc: ScioContext): SnowflakeScioContextOps =
    new SnowflakeScioContextOps(sc)
}
