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

import java.lang.{Iterable => JIterable}
import java.net.InetSocketAddress

import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.BulkExecutionException
import org.apache.beam.sdk.io.{elasticsearch => esio}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.elasticsearch.action.ActionRequest
import org.joda.time.Duration

import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
 * Main package for Elasticsearch APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.elasticsearch._
 * }}}
 */
package object elasticsearch {

  case class ElasticsearchIO[T](options: ElasticsearchOptions)
    extends TestIO[T](options.toString)

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
      if (self.context.isTest) {
        self.context.testOut(ElasticsearchIO[T](esOptions))(self)
      } else {
        val shards = if (numOfShards > 0) numOfShards else esOptions.servers.size
        self.applyInternal(
          esio.ElasticsearchIO.Write
            .withClusterName(esOptions.clusterName)
            .withServers(esOptions.servers.toArray)
            .withFunction(new SerializableFunction[T, JIterable[ActionRequest[_]]]() {
              override def apply(t: T): JIterable[ActionRequest[_]] = f(t).asJava
            })
            .withFlushInterval(flushInterval)
            .withNumOfShard(shards)
            .withMaxBulkRequestSize(maxBulkRequestSize)
            .withError(new esio.ThrowingConsumer[BulkExecutionException] {
              override def accept(t: BulkExecutionException): Unit = errorFn(t)
            }))
      }
      Future.failed(new NotImplementedError("Custom future not implemented"))
    }
  }

}
