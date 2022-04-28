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

package com.spotify.scio

import java.net.InetSocketAddress

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import com.spotify.scio.elasticsearch.ElasticsearchIO.{RetryConfig, WriteParam}
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.BulkExecutionException
import org.elasticsearch.action.DocWriteRequest
import org.joda.time.Duration

/**
 * Main package for Elasticsearch APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.elasticsearch._
 * }}}
 */
package object elasticsearch extends CoderInstances {
  final case class ElasticsearchOptions(clusterName: String, servers: Seq[InetSocketAddress])

  implicit
  class ElasticsearchSCollection[T](@transient private val self: SCollection[T]) extends AnyVal {

    /**
     * Save this SCollection into Elasticsearch.
     *
     * @param esOptions
     *   Elasticsearch options
     * @param flushInterval
     *   delays to Elasticsearch writes for rate limiting purpose
     * @param f
     *   function to transform arbitrary type T to Elasticsearch `DocWriteRequest`
     * @param numOfShards
     *   number of parallel writes to be performed, recommended setting is the number of pipeline
     *   workers
     * @param errorFn
     *   function to handle error when performing Elasticsearch bulk writes
     */
    @Deprecated("Elasticsearch 6 reached End of Life")
    def saveAsElasticsearch(
      esOptions: ElasticsearchOptions,
      flushInterval: Duration = WriteParam.DefaultFlushInterval,
      numOfShards: Long = WriteParam.DefaultNumShards,
      maxBulkRequestSize: Int = WriteParam.DefaultMaxBulkRequestSize,
      maxBulkRequestBytes: Long = WriteParam.DefaultMaxBulkRequestBytes,
      errorFn: BulkExecutionException => Unit = WriteParam.DefaultErrorFn,
      retry: RetryConfig = WriteParam.DefaultRetryConfig
    )(f: T => Iterable[DocWriteRequest[_]]): ClosedTap[Nothing] = {
      val param = WriteParam(
        f,
        errorFn,
        flushInterval,
        numOfShards,
        maxBulkRequestSize,
        maxBulkRequestBytes,
        retry
      )
      self.write(ElasticsearchIO[T](esOptions))(param)
    }
  }
}
