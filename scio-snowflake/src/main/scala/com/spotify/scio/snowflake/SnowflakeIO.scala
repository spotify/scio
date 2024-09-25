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

package com.spotify.scio.snowflake

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT}
import com.spotify.scio.values.SCollection
import kantan.csv.{RowDecoder, RowEncoder}
import org.apache.beam.sdk.io.snowflake.SnowflakeIO.{CsvMapper, UserDataMapper}
import org.apache.beam.sdk.io.{snowflake => beam}

object SnowflakeIO {

  private[snowflake] def dataSourceConfiguration(connectionOptions: SnowflakeConnectionOptions) = {

    val datasourceInitial = beam.SnowflakeIO.DataSourceConfiguration
      .create()

    val datasourceWithAuthent = connectionOptions.authenticationOptions match {
      case SnowflakeUsernamePasswordAuthenticationOptions(username, password) =>
        datasourceInitial.withUsernamePasswordAuth(username, password)
      case SnowflakeKeyPairAuthenticationOptions(username, privateKeyPath, None) =>
        datasourceInitial.withKeyPairPathAuth(username, privateKeyPath)
      case SnowflakeKeyPairAuthenticationOptions(username, privateKeyPath, Some(passphrase)) =>
        datasourceInitial.withKeyPairPathAuth(username, privateKeyPath, passphrase)
      case SnowflakeOAuthTokenAuthenticationOptions(token) =>
        datasourceInitial.withOAuth(token)
    }

    val datasourceBeforeSchema = datasourceWithAuthent
      .withServerName(connectionOptions.serverName)
      .withDatabase(connectionOptions.database)
      .withRole(connectionOptions.role)
      .withWarehouse(connectionOptions.warehouse)

    connectionOptions.schema
      .map(schema => datasourceBeforeSchema.withSchema(schema))
      .getOrElse(datasourceBeforeSchema)
  }

  private[snowflake] def buildCsvMapper[T](rowDecoder: RowDecoder[T]): CsvMapper[T] =
    new CsvMapper[T] {
      override def mapRow(parts: Array[String]): T = {
        val unsnowedParts = parts.map {
          case "\\N" => "" // needs to be mapped to an Option
          case other => other
        }.toSeq
        rowDecoder.unsafeDecode(unsnowedParts)
      }
    }

  private[snowflake] def prepareRead[T](
    snowflakeOptions: SnowflakeOptions,
    sc: ScioContext
  )(implicit rowDecoder: RowDecoder[T], coder: Coder[T]): beam.SnowflakeIO.Read[T] =
    beam.SnowflakeIO
      .read()
      .withDataSourceConfiguration(
        SnowflakeIO.dataSourceConfiguration(snowflakeOptions.connectionOptions)
      )
      .withStagingBucketName(snowflakeOptions.stagingBucketName)
      .withStorageIntegrationName(snowflakeOptions.storageIntegrationName)
      .withCsvMapper(buildCsvMapper(rowDecoder))
      .withCoder(CoderMaterializer.beam(sc, coder))
}

sealed trait SnowflakeIO[T] extends ScioIO[T]

final case class SnowflakeSelect[T](snowflakeOptions: SnowflakeOptions, select: String)(implicit
  rowDecoder: RowDecoder[T],
  coder: Coder[T]
) extends SnowflakeIO[T] {

  override type ReadP = Unit
  override type WriteP = Unit
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.applyTransform(SnowflakeIO.prepareRead(snowflakeOptions, sc).fromQuery(select))

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("SnowflakeSelect is read-only")

  override def tap(params: ReadP): Tap[Nothing] = EmptyTap
}

final case class SnowflakeTable[T](snowflakeOptions: SnowflakeOptions, table: String)(implicit
  rowDecoder: RowDecoder[T],
  rowEncoder: RowEncoder[T],
  coder: Coder[T]
) extends SnowflakeIO[T] {

  override type ReadP = Unit
  override type WriteP = Unit
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.applyTransform(SnowflakeIO.prepareRead(snowflakeOptions, sc).fromTable(table))

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    data.applyInternal(
      beam.SnowflakeIO
        .write[T]()
        .withDataSourceConfiguration(
          SnowflakeIO.dataSourceConfiguration(snowflakeOptions.connectionOptions)
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

  override def tap(params: ReadP): Tap[Nothing] = EmptyTap
}
