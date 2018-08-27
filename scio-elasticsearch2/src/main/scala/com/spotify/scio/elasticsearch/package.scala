/*
 * Copyright 2017 Spotify AB.
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

import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.BulkExecutionException
import org.elasticsearch.action.ActionRequest
import org.joda.time.Duration

import scala.concurrent.Future

/**
 * Main package for Elasticsearch APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.elasticsearch._
 * }}}
 */
package object elasticsearch {

  type ElasticsearchIO[T] = elasticsearch.nio.ElasticsearchIO[T]
  val ElasticsearchIO = elasticsearch.nio.ElasticsearchIO

  case class ElasticsearchOptions(clusterName: String, servers: Seq[InetSocketAddress])

  implicit class ElasticsearchSCollection[T](val self: SCollection[T]) extends AnyVal {

    /**
     * Save this SCollection into Elasticsearch.
     *
     * @param esOptions Elasticsearch options
     * @param flushInterval delays to Elasticsearch writes for rate limiting purpose
     * @param f function to transform arbitrary type T to Elasticsearch [[ActionRequest]]
     * @param numOfShards number of parallel writes to be performed, recommended setting is the
     *                   number of pipeline workers
     * @param errorFn function to handle error when performing Elasticsearch bulk writes
     */
    def saveAsElasticsearch(esOptions: ElasticsearchOptions,
                            flushInterval: Duration = Duration.standardSeconds(1),
                            numOfShards: Long = 0,
                            maxBulkRequestSize: Int = 3000,
                            errorFn: BulkExecutionException => Unit = m => throw m)
                           (f: T => Iterable[ActionRequest[_]]): Future[Tap[T]] = {
      val io = ElasticsearchIO[T](esOptions)
      val param = ElasticsearchIO.WriteParam(
        f, errorFn, flushInterval, numOfShards, maxBulkRequestSize)
      self.write(io)(param)
    }
  }

}

