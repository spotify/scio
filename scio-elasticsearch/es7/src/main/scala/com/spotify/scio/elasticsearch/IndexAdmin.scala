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

import org.apache.http.HttpHost
import org.elasticsearch.client._
import org.elasticsearch.client.indices.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.common.xcontent.XContentType

import scala.util.Try

object IndexAdmin {

  private def indicesClient[A](esOptions: ElasticsearchOptions)(f: IndicesClient => A): Try[A] = {
    val client = new RestHighLevelClient(RestClient.builder(esOptions.nodes: _*))

    val result = Try(f(client.indices()))
    client.close()
    result
  }

  /**
   * Ensure that index is created.
   * If index already exists or some other error occurs this results in a [[scala.util.Failure]].
   *
   * @param index index to be created
   * @param mappingSource a valid json string
   */
  def ensureIndex(
    nodes: Iterable[HttpHost],
    index: String,
    mappingSource: String
  ): Try[CreateIndexResponse] = {
    val esOptions = ElasticsearchOptions(nodes.toSeq)
    ensureIndex(esOptions, index, mappingSource)
  }

  /**
   * Ensure that index is created.
   * If index already exists or some other error occurs this results in a [[scala.util.Failure]].
   *
   * @param index index to be created
   * @param mappingSource a valid json string
   */
  def ensureIndex(
    esOptions: ElasticsearchOptions,
    index: String,
    mappingSource: String
  ): Try[CreateIndexResponse] =
    indicesClient(esOptions) { client =>
      ensureIndex(index, mappingSource, client)
    }

  /**
   * Ensure that index is created.
   *
   * @param index index to be created
   * @param mappingSource a valid json string
   */
  private def ensureIndex(
    index: String,
    mappingSource: String,
    client: IndicesClient
  ): CreateIndexResponse = {
    client.create(
      new CreateIndexRequest(index).source(mappingSource, XContentType.JSON),
      RequestOptions.DEFAULT
    )
  }

}
