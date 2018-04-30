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
import org.elasticsearch.action.DocWriteRequest
import org.apache.beam.sdk.io.{elasticsearch => esio}
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.BulkExecutionException
import org.apache.beam.sdk.transforms.SerializableFunction
import org.joda.time.Duration
import scala.concurrent.Future
import java.lang.{Iterable => JIterable}
import scala.collection.JavaConverters._

final case class ElacticsearchIO[T](
                            esOptions: ElasticsearchOptions,
                            flushInterval: Duration = Duration.standardSeconds(1),
                            numOfShards: Long = 0,
                            maxBulkRequestSize: Int = 3000,
                            errorFn: BulkExecutionException => Unit = m => throw m)
                           (f: T => Iterable[DocWriteRequest[_]])
  extends ScioIO[T] {

  type ReadP = Nothing
  type WriteP = Unit

  def id: String = esOptions.toString

  def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new NotImplementedError("Can't read from Elacticsearch")

  def tap(read: ReadP): Tap[T] =
    throw new NotImplementedError("Can't read from Elacticsearch")

  /**
   * Save this SCollection into Elasticsearch.
   *
   * @param esOptions Elasticsearch options
   * @param flushInterval delays to Elasticsearch writes for rate limiting purpose
   * @param f function to transform arbitrary type T to Elasticsearch `DocWriteRequest`
   * @param numOfShards number of parallel writes to be performed, recommended setting is the
   *                   number of pipeline workers
   * @param errorFn function to handle error when performing Elasticsearch bulk writes
   */
  def write(sc: SCollection[T], params: WriteP): Future[Tap[T]] = {
    val shards = if (numOfShards > 0) numOfShards else esOptions.servers.size
    sc.applyInternal(
      esio.ElasticsearchIO.Write
        .withClusterName(esOptions.clusterName)
        .withServers(esOptions.servers.toArray)
        .withFunction(new SerializableFunction[T, JIterable[DocWriteRequest[_]]]() {
          override def apply(t: T): JIterable[DocWriteRequest[_]] = f(t).asJava
        })
        .withFlushInterval(flushInterval)
        .withNumOfShard(shards)
        .withMaxBulkRequestSize(maxBulkRequestSize)
        .withError(new esio.ThrowingConsumer[BulkExecutionException] {
          override def accept(t: BulkExecutionException): Unit = errorFn(t)
        }))
    Future.failed(new NotImplementedError("Custom future not implemented"))
  }
}
