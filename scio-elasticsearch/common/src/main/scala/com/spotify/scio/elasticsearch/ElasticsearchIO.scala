/*
 * Copyright 2022 Spotify AB.
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

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import com.spotify.scio.ScioContext
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.BulkExecutionException
import org.apache.beam.sdk.io.{elasticsearch => beam}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.http.auth.UsernamePasswordCredentials
import org.joda.time.Duration

import java.lang.{Iterable => JIterable}
import scala.jdk.CollectionConverters._
import scala.util.chaining._

final case class ElasticsearchIO[T](esOptions: ElasticsearchOptions) extends ScioIO[T] {
  override type ReadP = Nothing
  override type WriteP = ElasticsearchIO.WriteParam[T]
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  // remove the mapperFactory from the test id
  override def testId: String =
    s"ElasticsearchIO(${esOptions.nodes}, ${esOptions.usernameAndPassword})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("Can't read from Elasticsearch")

  /** Save this SCollection into Elasticsearch. */
  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val shards = if (params.numOfShards >= 0) params.numOfShards else esOptions.nodes.size.toLong
    val credentials = esOptions.usernameAndPassword.map { case (username, password) =>
      new UsernamePasswordCredentials(username, password)
    }

    val write = beam.ElasticsearchIO.Write
      .withNodes(esOptions.nodes.toArray)
      .withFunction(new SerializableFunction[T, JIterable[BulkOperation]]() {
        override def apply(t: T): JIterable[BulkOperation] = params.f(t).asJava
      })
      .withFlushInterval(params.flushInterval)
      .withNumOfShard(shards)
      .withMaxBulkRequestOperations(params.maxBulkRequestOperations)
      .withMaxBulkRequestBytes(params.maxBulkRequestBytes)
      .withMaxRetries(params.retry.maxRetries)
      .withRetryPause(params.retry.retryPause)
      .withError((t: BulkExecutionException) => params.errorFn(t))
      .pipe(w => credentials.map(w.withCredentials).getOrElse(w))
      .withMapperFactory(() => esOptions.mapperFactory.apply())

    data.applyInternal(write)
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object ElasticsearchIO {
  object WriteParam {
    val DefaultErrorFn: BulkExecutionException => Unit = m => throw m
    val DefaultFlushInterval: Duration = Duration.standardSeconds(1)
    val DefaultNumShards: Long = -1L
    val DefaultMaxBulkRequestOperations: Int = 3000
    val DefaultMaxBulkRequestBytes: Long = 5L * 1024L * 1024L
    val DefaultMaxRetries: Int = 3
    val DefaultRetryPause: Duration = Duration.millis(35000)
    val DefaultRetryConfig: RetryConfig = RetryConfig(
      maxRetries = WriteParam.DefaultMaxRetries,
      retryPause = WriteParam.DefaultRetryPause
    )
  }
  final case class WriteParam[T] private (
    f: T => Iterable[BulkOperation],
    errorFn: BulkExecutionException => Unit = WriteParam.DefaultErrorFn,
    flushInterval: Duration = WriteParam.DefaultFlushInterval,
    numOfShards: Long = WriteParam.DefaultNumShards,
    maxBulkRequestOperations: Int = WriteParam.DefaultMaxBulkRequestOperations,
    maxBulkRequestBytes: Long = WriteParam.DefaultMaxBulkRequestBytes,
    retry: RetryConfig = WriteParam.DefaultRetryConfig
  )

  final case class RetryConfig(maxRetries: Int, retryPause: Duration)
}
