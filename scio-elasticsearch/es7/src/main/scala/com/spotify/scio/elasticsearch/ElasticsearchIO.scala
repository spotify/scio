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

package com.spotify.scio.elasticsearch

import java.lang.{Iterable => JIterable}

import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap}
import org.elasticsearch.action.DocWriteRequest
import org.apache.beam.sdk.io.{elasticsearch => beam}
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.BulkExecutionException
import org.apache.beam.sdk.transforms.SerializableFunction
import org.joda.time.Duration

import scala.jdk.CollectionConverters._
import com.spotify.scio.io.TapT
import org.apache.http.auth.UsernamePasswordCredentials

final case class ElasticsearchIO[T](esOptions: ElasticsearchOptions) extends ScioIO[T] {
  override type ReadP = Nothing
  override type WriteP = ElasticsearchIO.WriteParam[T]
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("Can't read from Elasticsearch")

  /** Save this SCollection into Elasticsearch. */
  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val shards = if (params.numOfShards >= 0) {
      params.numOfShards
    } else {
      esOptions.nodes.size
    }

    val write = beam.ElasticsearchIO.Write
      .withNodes(esOptions.nodes.toArray)
      .withFunction(new SerializableFunction[T, JIterable[DocWriteRequest[_]]]() {
        override def apply(t: T): JIterable[DocWriteRequest[_]] =
          params.f(t).asJava
      })
      .withFlushInterval(params.flushInterval)
      .withNumOfShard(shards)
      .withMaxBulkRequestSize(params.maxBulkRequestSize)
      .withMaxBulkRequestBytes(params.maxBulkRequestBytes)
      .withMaxRetries(params.retry.maxRetries)
      .withRetryPause(params.retry.retryPause)
      .withError((t: BulkExecutionException) => params.errorFn(t))

    data.applyInternal(
      esOptions.usernameAndPassword
        .map { case (username, password) =>
          write.withCredentials(new UsernamePasswordCredentials(username, password))
        }
        .getOrElse(write)
    )

    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object ElasticsearchIO {
  object WriteParam {
    private[elasticsearch] val DefaultErrorFn: BulkExecutionException => Unit = m => throw m
    private[elasticsearch] val DefaultFlushInterval = Duration.standardSeconds(1)
    private[elasticsearch] val DefaultNumShards = -1
    private[elasticsearch] val DefaultMaxBulkRequestSize = 3000
    private[elasticsearch] val DefaultMaxBulkRequestBytes = 5L * 1024L * 1024L
    private[elasticsearch] val DefaultMaxRetries = 3
    private[elasticsearch] val DefaultRetryPause = Duration.millis(35000)
    private[elasticsearch] val DefaultRetryConfig =
      RetryConfig(
        maxRetries = WriteParam.DefaultMaxRetries,
        retryPause = WriteParam.DefaultRetryPause
      )
  }

  final case class WriteParam[T] private (
    f: T => Iterable[DocWriteRequest[_]],
    errorFn: BulkExecutionException => Unit = WriteParam.DefaultErrorFn,
    flushInterval: Duration = WriteParam.DefaultFlushInterval,
    numOfShards: Long = WriteParam.DefaultNumShards,
    maxBulkRequestSize: Int = WriteParam.DefaultMaxBulkRequestSize,
    maxBulkRequestBytes: Long = WriteParam.DefaultMaxBulkRequestBytes,
    retry: RetryConfig = WriteParam.DefaultRetryConfig
  )

  final case class RetryConfig(maxRetries: Int, retryPause: Duration)
}
