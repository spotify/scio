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

import scala.util.chaining._
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT, TestIO}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import kantan.csv.{RowCodec, RowDecoder, RowEncoder}
import org.apache.beam.sdk.io.snowflake.SnowflakeIO.{CsvMapper, UserDataMapper}
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema
import org.apache.beam.sdk.io.snowflake.enums.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.{snowflake => beam}
import org.joda.time.Duration

object SnowflakeIO {

  final def apply[T](opts: SnowflakeConnectionOptions, query: String): SnowflakeIO[T] =
    new SnowflakeIO[T] with TestIO[T] {
      final override val tapT = EmptyTapOf[T]
      override def testId: String = s"SnowflakeIO(${snowflakeIoId(opts, query)})"
    }

  private[snowflake] def snowflakeIoId(opts: SnowflakeConnectionOptions, target: String): String = {
    // source params
    val params = Option(opts.database).map(db => s"db=$db") ++
      Option(opts.warehouse).map(db => s"warehouse=$db")
    s"${opts.url}${params.mkString("?", "&", "")}:$target"
  }

  object ReadParam {
    type ConfigOverride[T] = beam.SnowflakeIO.Read[T] => beam.SnowflakeIO.Read[T]

    val DefaultStagingBucketName: String = null
    val DefaultQuotationMark: String = null
    val DefaultConfigOverride = null
  }
  final case class ReadParam[T](
    storageIntegrationName: String,
    stagingBucketName: String = ReadParam.DefaultStagingBucketName,
    quotationMark: String = ReadParam.DefaultQuotationMark,
    configOverride: ReadParam.ConfigOverride[T] = ReadParam.DefaultConfigOverride
  )

  object WriteParam {
    type ConfigOverride[T] = beam.SnowflakeIO.Write[T] => beam.SnowflakeIO.Write[T]

    val DefaultTableSchema: SnowflakeTableSchema = null
    val DefaultCreateDisposition: CreateDisposition = null
    val DefaultWriteDisposition: WriteDisposition = null
    val DefaultSnowPipe: String = null
    val DefaultShardNumber: Integer = null
    val DefaultFlushRowLimit: Integer = null
    val DefaultFlushTimeLimit: Duration = null
    val DefaultStorageIntegrationName: String = null
    val DefaultStagingBucketName: String = null
    val DefaultQuotationMark: String = null
    val DefaultConfigOverride = null
  }
  final case class WriteParam[T](
    tableSchema: SnowflakeTableSchema = WriteParam.DefaultTableSchema,
    createDisposition: CreateDisposition = WriteParam.DefaultCreateDisposition,
    writeDisposition: WriteDisposition = WriteParam.DefaultWriteDisposition,
    snowPipe: String = WriteParam.DefaultSnowPipe,
    shardNumber: Integer = WriteParam.DefaultShardNumber,
    flushRowLimit: Integer = WriteParam.DefaultFlushRowLimit,
    flushTimeLimit: Duration = WriteParam.DefaultFlushTimeLimit,
    storageIntegrationName: String = WriteParam.DefaultStorageIntegrationName,
    stagingBucketName: String = WriteParam.DefaultStagingBucketName,
    quotationMark: String = WriteParam.DefaultQuotationMark,
    configOverride: WriteParam.ConfigOverride[T] = WriteParam.DefaultConfigOverride
  )

  private[snowflake] def dataSourceConfiguration(connectionOptions: SnowflakeConnectionOptions) =
    beam.SnowflakeIO.DataSourceConfiguration
      .create()
      .withUrl(connectionOptions.url)
      .pipe { ds =>
        import SnowflakeAuthenticationOptions._
        Option(connectionOptions.authenticationOptions).fold(ds) {
          case UsernamePassword(username, password) =>
            ds.withUsernamePasswordAuth(username, password)
          case KeyPair(username, privateKeyPath, None) =>
            ds.withKeyPairPathAuth(username, privateKeyPath)
          case KeyPair(username, privateKeyPath, Some(passphrase)) =>
            ds.withKeyPairPathAuth(username, privateKeyPath, passphrase)
          case OAuthToken(token) =>
            ds.withOAuth(token).withAuthenticator("oauth")
        }
      }
      .pipe(ds => Option(connectionOptions.database).fold(ds)(ds.withDatabase))
      .pipe(ds => Option(connectionOptions.role).fold(ds)(ds.withRole))
      .pipe(ds => Option(connectionOptions.warehouse).fold(ds)(ds.withWarehouse))
      .pipe(ds =>
        Option(connectionOptions.loginTimeout)
          .map[Integer](_.getStandardSeconds.toInt)
          .fold(ds)(ds.withLoginTimeout)
      )
      .pipe(ds => Option(connectionOptions.schema).fold(ds)(ds.withSchema))

  private[snowflake] def csvMapper[T: RowDecoder]: CsvMapper[T] = { (parts: Array[String]) =>
    val unsnowedParts = parts.map {
      case "\\N" => "" // needs to be mapped to an Option
      case other => other
    }.toSeq
    RowDecoder[T].unsafeDecode(unsnowedParts)
  }

  private[snowflake] def userDataMapper[T: RowEncoder]: UserDataMapper[T] = { (element: T) =>
    RowEncoder[T].encode(element).toArray
  }
}

sealed trait SnowflakeIO[T] extends ScioIO[T]

final case class SnowflakeSelect[T](connectionOptions: SnowflakeConnectionOptions, query: String)(
  implicit
  rowDecoder: RowDecoder[T],
  coder: Coder[T]
) extends SnowflakeIO[T] {

  import SnowflakeIO._

  override type ReadP = ReadParam[T]
  override type WriteP = Unit
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"SnowflakeIO(${snowflakeIoId(connectionOptions, query)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val tempDirectory = ScioUtil.tempDirOrDefault(params.stagingBucketName, sc).toString
    val t = beam.SnowflakeIO
      .read[T]()
      .fromQuery(query)
      .withDataSourceConfiguration(dataSourceConfiguration(connectionOptions))
      .withStorageIntegrationName(params.storageIntegrationName)
      .withStagingBucketName(tempDirectory)
      .pipe(r => Option(params.quotationMark).fold(r)(r.withQuotationMark))
      .withCsvMapper(csvMapper)
      .withCoder(CoderMaterializer.beam(sc, coder))
      .pipe(r => Option(params.configOverride).fold(r)(_(r)))

    sc.applyTransform(t)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("SnowflakeSelect is read-only")

  override def tap(params: ReadP): Tap[Nothing] = EmptyTap
}

final case class SnowflakeTable[T](connectionOptions: SnowflakeConnectionOptions, table: String)(
  implicit
  rowCodec: RowCodec[T], // use codec for tap
  coder: Coder[T]
) extends SnowflakeIO[T] {

  import SnowflakeIO._

  override type ReadP = ReadParam[T]
  override type WriteP = WriteParam[T]
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T] // TODO Create a tap

  override def testId: String = s"SnowflakeIO(${snowflakeIoId(connectionOptions, table)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val tempDirectory = ScioUtil.tempDirOrDefault(params.stagingBucketName, sc).toString
    val t = beam.SnowflakeIO
      .read[T]()
      .fromTable(table)
      .withDataSourceConfiguration(dataSourceConfiguration(connectionOptions))
      .withStorageIntegrationName(params.storageIntegrationName)
      .withStagingBucketName(tempDirectory)
      .pipe(r => Option(params.quotationMark).fold(r)(r.withQuotationMark))
      .withCsvMapper(csvMapper)
      .withCoder(CoderMaterializer.beam(sc, coder))
      .pipe(r => Option(params.configOverride).fold(r)(_(r)))

    sc.applyTransform(t)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val tempDirectory = ScioUtil.tempDirOrDefault(params.stagingBucketName, data.context).toString
    val t = beam.SnowflakeIO
      .write[T]()
      .withDataSourceConfiguration(dataSourceConfiguration(connectionOptions))
      .to(table)
      .pipe(w => Option(params.createDisposition).fold(w)(w.withCreateDisposition))
      .pipe(w => Option(params.writeDisposition).fold(w)(w.withWriteDisposition))
      .pipe(w => Option(params.snowPipe).fold(w)(w.withSnowPipe))
      .pipe(w => Option(params.shardNumber).fold(w)(w.withShardsNumber))
      .pipe(w => Option(params.flushRowLimit).fold(w)(w.withFlushRowLimit))
      .pipe(w => Option(params.flushTimeLimit).fold(w)(w.withFlushTimeLimit))
      .pipe(w => Option(params.quotationMark).fold(w)(w.withQuotationMark))
      .pipe(w => Option(params.storageIntegrationName).fold(w)(w.withStorageIntegrationName))
      .pipe(w => Option(params.tableSchema).fold(w)(w.withTableSchema))
      .withStagingBucketName(tempDirectory)
      .withUserDataMapper(userDataMapper)
      .pipe(w => Option(params.configOverride).fold(w)(_(w)))

    data.applyInternal(t)
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] = EmptyTap
}
