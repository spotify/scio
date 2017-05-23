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

import org.joda.time.Duration
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.SerializableFunction
import org.elasticsearch.action.index.IndexRequest

import scala.concurrent.Future

/**
  * Main package for Elasticsearch APIs. Import all.
  *
  * {{{
  * import com.spotify.scio.elasticsearch._
  * }}}
  */
package object elasticsearch {

  case class ElasticsearchIOTest[T](options: ElasticsearchOptions)
    extends TestIO[T](options.toString)

  case class ElasticsearchOptions(clusterName: String, servers: Array[InetSocketAddress])
  implicit class ElasticsearchSCollection[T](val self: SCollection[T])
    extends AnyVal {
    /**
      * Save this SCollection into Elasticsearch.
      *
      * @param elasticsearchOptions defines clusterName and cluster endpoints
      * @param flushInterval delays writes to Elasticsearch cluster to rate limit
      * @param f transforms arbitrary type T to the object required by Elasticsearch client
      * @param numOfShard number of parallel writes to be performed.
      *                   Note: Recommended to be equal to number of workers in your pipeline.
      */
    def saveAsElasticsearch(elasticsearchOptions: ElasticsearchOptions,
                            flushInterval: Duration = Duration.standardSeconds(1),
                            f: T => IndexRequest,
                            numOfShard: Long) :Future[Tap[T]] = {

      if (self.context.isTest) {
        self.context.testOut(
          ElasticsearchIOTest[T](elasticsearchOptions))(self)
      } else {
        self.applyInternal(
          ElasticsearchIO.Write
            .withClusterName(elasticsearchOptions.clusterName)
            .withServers(elasticsearchOptions.servers)
            .withNumOfShard(numOfShard)
            .withFunction(new SerializableFunction[T, IndexRequest]() {
              override def apply(t: T): IndexRequest = f(t)
            }))
      }
      Future.failed(new NotImplementedError("Custom future not implemented"))
    }
  }
}

