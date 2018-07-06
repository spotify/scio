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

package com.spotify.scio.elasticsearch.nio

import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.io.Tap
import com.spotify.scio.elasticsearch.ElasticsearchOptions
import org.elasticsearch.action.ActionRequest
import org.apache.beam.sdk.io.{elasticsearch => esio}
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.BulkExecutionException
import org.apache.beam.sdk.transforms.SerializableFunction
import org.joda.time.Duration
import scala.concurrent.Future
import java.lang.{Iterable => JIterable}
import scala.collection.JavaConverters._

final case class ElasticsearchIO[T](esOptions: ElasticsearchOptions)
  extends ScioIO[T] {

  case class WriteParams(f: T => Iterable[ActionRequest[_]],
                         errorFn: BulkExecutionException => Unit = m => throw m,
                         flushInterval: Duration = Duration.standardSeconds(1),
                         numOfShards: Long = 0,
                         maxBulkRequestSize: Int = 3000)

  type ReadP = Nothing
  type WriteP = WriteParams

  def id: String = esOptions.toString

  def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new NotImplementedError("Can't read from ElasticSearch")

  /**
   * Save this SCollection into Elasticsearch.
   */
  def write(sc: SCollection[T], params: WriteP): Future[Tap[T]] = {
    val shards = if (params.numOfShards > 0) params.numOfShards else esOptions.servers.size
    sc.applyInternal(
      esio.ElasticsearchIO.Write
        .withClusterName(esOptions.clusterName)
        .withServers(esOptions.servers.toArray)
        .withFunction(new SerializableFunction[T, JIterable[ActionRequest[_]]]() {
          override def apply(t: T): JIterable[ActionRequest[_]] = params.f(t).asJava
        })
        .withFlushInterval(params.flushInterval)
        .withNumOfShard(shards)
        .withMaxBulkRequestSize(params.maxBulkRequestSize)
        .withError(new esio.ThrowingConsumer[BulkExecutionException] {
          override def accept(t: BulkExecutionException): Unit = params.errorFn(t)
        }))
    Future.failed(new NotImplementedError("Custom future not implemented"))
  }

  def tap(read: ReadP): Tap[T] =
    throw new NotImplementedError("Can't read from ElasticSearch")
}
