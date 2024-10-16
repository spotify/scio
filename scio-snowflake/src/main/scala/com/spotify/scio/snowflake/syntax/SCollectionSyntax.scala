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
import com.spotify.scio.io.{EmptyTap, Tap}
import com.spotify.scio.snowflake.SnowflakeOptions
import com.spotify.scio.values.SCollection
import kantan.csv.{RowDecoder, RowEncoder}
import org.apache.beam.sdk.io.snowflake.SnowflakeIO.UserDataMapper
import org.apache.beam.sdk.io.{snowflake => beam}

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Snowflake methods.
 */
final class SnowflakeSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  import com.spotify.scio.snowflake.SnowflakeIO._

  /**
   * Execute the provided SQL query in Snowflake, COPYing the result in CSV format to the provided
   * bucket, and return an [[SCollection]] of provided type, reading this bucket.
   *
   * The [[SCollection]] is generated using [[kantan.csv.RowDecoded]]. [[SCollection]] type
   * properties must then match the order of the columns of the SELECT, that will be copied to the
   * bucket.
   *
   * @see
   *   ''Reading from Snowflake'' in the
   *   [[https://beam.apache.org/documentation/io/built-in/snowflake/ Beam `SnowflakeIO` documentation]]
   * @param snowflakeConf
   *   options for configuring a Snowflake integration
   * @param query
   *   SQL select query
   * @return
   *   [[SCollection]] containing the query results as parsed from the CSV bucket copied from
   *   Snowflake
   */
  def snowflakeSelect[U](
    snowflakeConf: SnowflakeOptions,
    query: String
  )(implicit
    rowDecoder: RowDecoder[U],
    coder: Coder[U]
  ): SCollection[U] =
    self.context.applyTransform(prepareRead(snowflakeConf, self.context).fromQuery(query))

  /**
   * Copy the provided Snowflake table in CSV format to the provided bucket, and * return an
   * [[SCollection]] of provided type, reading this bucket.
   *
   * The [[SCollection]] is generated using [[kantan.csv.RowDecoded]]. [[SCollection]] type
   * properties must then match the order of the columns of the table, that will be copied to the
   * bucket.
   *
   * @see
   *   ''Reading from Snowflake'' in the
   *   [[https://beam.apache.org/documentation/io/built-in/snowflake/ Beam `SnowflakeIO` documentation]]
   * @param snowflakeConf
   *   options for configuring a Snowflake integration
   * @param table
   *   table
   * @return
   *   [[SCollection]] containing the table elements as parsed from the CSV bucket copied from
   *   Snowflake table
   */
  def snowflakeTable[U](
    snowflakeConf: SnowflakeOptions,
    table: String
  )(implicit
    rowDecoder: RowDecoder[U],
    coder: Coder[U]
  ): SCollection[U] =
    self.context.applyTransform(prepareRead(snowflakeConf, self.context).fromTable(table))

  /**
   * Save this SCollection as a Snowflake database table. The [[SCollection]] is written to CSV
   * files in a bucket, using the provided [[kantan.csv.RowEncoder]] to encode each element as a CSV
   * row. The bucket is then COPYied to the Snowflake table.
   *
   * @see
   *   ''Writing to Snowflake tables'' in the
   *   [[https://beam.apache.org/documentation/io/built-in/snowflake/ Beam `SnowflakeIO` documentation]]
   *
   * @param snowflakeOptions
   *   options for configuring a Snowflake connexion
   * @param table
   *   Snowflake table
   */
  def saveAsSnowflakeTable(
    snowflakeOptions: SnowflakeOptions,
    table: String
  )(implicit rowEncoder: RowEncoder[T], coder: Coder[T]): Tap[Nothing] = {
    self.applyInternal(
      beam.SnowflakeIO
        .write[T]()
        .withDataSourceConfiguration(
          dataSourceConfiguration(snowflakeOptions.connectionOptions)
        )
        .to(table)
        .withStagingBucketName(snowflakeOptions.stagingBucketName)
        .withStorageIntegrationName(snowflakeOptions.storageIntegrationName)
        .withUserDataMapper(new UserDataMapper[T] {
          override def mapRow(element: T): Array[AnyRef] = rowEncoder.encode(element).toArray
        })
    )
    EmptyTap
  }
}

trait SCollectionSyntax {
  implicit def snowflakeSCollectionOps[T](sc: SCollection[T]): SnowflakeSCollectionOps[T] =
    new SnowflakeSCollectionOps(sc)
}
