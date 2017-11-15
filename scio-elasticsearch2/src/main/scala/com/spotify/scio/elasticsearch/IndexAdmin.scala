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

package com.spotify.scio.elasticsearch

import java.net.InetSocketAddress

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.client.AdminClient
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.util.Try

object IndexAdmin {

  private def adminClient[A](esOptions: ElasticsearchOptions)(f: AdminClient => A): Try[A] = {
    val settings: Settings =
      Settings.settingsBuilder.put("cluster.name", esOptions.clusterName).build

    val transportAddresses: Seq[InetSocketTransportAddress] = esOptions.servers
      .map(addr => new InetSocketTransportAddress(addr))

    val client = TransportClient.builder
      .settings(settings)
      .build
      .addTransportAddresses(transportAddresses: _*)

    val result = Try(f(client.admin()))
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
  def ensureIndex(cluster: String,
                  servers: Iterable[InetSocketAddress],
                  index: String,
                  mappingSource: String): Try[CreateIndexResponse] = {
    val esOptions = ElasticsearchOptions(cluster, servers.toSeq)
    ensureIndex(esOptions, index, mappingSource)
  }

  /**
   * Ensure that index is created.
   * If index already exists or some other error occurs this results in a [[scala.util.Failure]].
   *
   * @param index index to be created
   * @param mappingSource a valid json string
   */
  def ensureIndex(esOptions: ElasticsearchOptions,
                  index: String,
                  mappingSource: String): Try[CreateIndexResponse] =
    adminClient(esOptions) { client =>
      ensureIndex(index, mappingSource, client)
    }

  /**
   * Ensure that index is created.
   *
   * @param index index to be created
   * @param mappingSource a valid json string
   */
  private def ensureIndex(index: String,
                          mappingSource: String,
                          client: AdminClient): CreateIndexResponse = {
    client
      .indices()
      .prepareCreate(index)
      .setSource(mappingSource)
      .get()
  }

}
