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

package com.spotify.scio.spanner

import com.google.cloud.spanner._
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode
import org.apache.beam.sdk.io.gcp.spanner.{SpannerConfig, SpannerIO => BSpannerIO}

import scala.jdk.CollectionConverters._
import com.spotify.scio.io.TapT

sealed trait SpannerIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]
  val config: SpannerConfig

  override def testId: String =
    s"${getClass.getSimpleName}" +
      s"(${config.getProjectId}, ${config.getInstanceId}, ${config.getDatabaseId})"
}

object SpannerRead {
  object ReadParam {
    private[spanner] val DefaultWithTransaction = false
    private[spanner] val DefaultWithBatching = true
  }

  sealed trait ReadMethod
  final case class FromTable(tableName: String, columns: Seq[String]) extends ReadMethod
  final case class FromQuery(query: String) extends ReadMethod

  final case class ReadParam private[spanner] (
    readMethod: ReadMethod,
    withTransaction: Boolean = ReadParam.DefaultWithTransaction,
    withBatching: Boolean = ReadParam.DefaultWithBatching
  )
}

final case class SpannerRead(config: SpannerConfig) extends SpannerIO[Struct] {
  import SpannerRead._

  override type ReadP = ReadParam
  override type WriteP = Nothing

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Struct] = {
    var transform = BSpannerIO
      .read()
      .withSpannerConfig(config)
      .withBatching(params.withBatching)

    transform = params.readMethod match {
      case x: FromTable => transform.withTable(x.tableName).withColumns(x.columns.asJava)
      case y: FromQuery => transform.withQuery(y.query)
    }

    if (params.withTransaction) {
      val txn = BSpannerIO.createTransaction().withSpannerConfig(config)
      transform = transform.withTransaction(sc.applyInternal(txn))
    }

    sc.applyTransform(transform)
  }

  override protected def write(data: SCollection[Struct], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("SpannerRead is read-only")

  override def tap(params: ReadP): Tap[Nothing] = EmptyTap
}

object SpannerWrite {
  object WriteParam {
    private[spanner] val DefaultFailureMode = FailureMode.FAIL_FAST
    private[spanner] val DefaultBatchSizeBytes = 1024L * 1024L
  }

  final case class WriteParam private[spanner] (
    failureMode: FailureMode = WriteParam.DefaultFailureMode,
    batchSizeBytes: Long = WriteParam.DefaultBatchSizeBytes
  )
}

final case class SpannerWrite(config: SpannerConfig) extends SpannerIO[Mutation] {
  override type ReadP = Nothing
  override type WriteP = SpannerWrite.WriteParam

  override protected def write(data: SCollection[Mutation], params: WriteP): Tap[Nothing] = {
    val transform = BSpannerIO
      .write()
      .withSpannerConfig(config)
      .withBatchSizeBytes(params.batchSizeBytes)
      .withFailureMode(params.failureMode)

    data.applyInternal(transform)
    EmptyTap
  }

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Mutation] = sc.wrap {
    throw new UnsupportedOperationException("SpannerWrite is write-only")
  }

  override def tap(params: ReadP): Tap[Nothing] = EmptyTap
}
