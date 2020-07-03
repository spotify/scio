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

import java.net.InetSocketAddress

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.AdminClient
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.client.PreBuiltTransportClient

import org.slf4j.LoggerFactory

import scala.util.Try
import scala.jdk.CollectionConverters._

object IndexAdmin {
  private[this] val Logger = LoggerFactory.getLogger(getClass)

  private def adminClient[A](esOptions: ElasticsearchOptions)(f: AdminClient => A): Try[A] = {
    val settings: Settings =
      Settings.builder.put("cluster.name", esOptions.clusterName).build

    val transportAddresses: Seq[InetSocketTransportAddress] = esOptions.servers
      .map(addr => new InetSocketTransportAddress(addr))

    val client = new PreBuiltTransportClient(settings)
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
  def ensureIndex(
    cluster: String,
    servers: Iterable[InetSocketAddress],
    index: String,
    mappingSource: String
  ): Try[CreateIndexResponse] = {
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
  def ensureIndex(
    esOptions: ElasticsearchOptions,
    index: String,
    mappingSource: String
  ): Try[CreateIndexResponse] =
    adminClient(esOptions)(client => ensureIndex(index, mappingSource, client))

  /**
   * Ensure that index is created.
   *
   * @param index index to be created
   * @param mappingSource a valid json string
   */
  private def ensureIndex(
    index: String,
    mappingSource: String,
    client: AdminClient
  ): CreateIndexResponse =
    client
      .indices()
      .prepareCreate(index)
      .setSource(mappingSource, XContentType.JSON)
      .get()

  /**
   * Add or update index alias with an option to remove the alias from all other indexes if it is already
   * pointed to any.
   *
   * @param alias            to be re-assigned
   * @param indices          Iterable of indicies to point the alias to
   * @param removePrevious   When set to true, the indexAlias would be removed from all indices it
   *                         was assigned to before adding new index alias assignment
   */
  def createOrUpdateAlias(
    client: IndicesAdminClient,
    indices: Iterable[String],
    alias: String,
    removePrevious: Boolean,
    timeout: TimeValue
  ): AcknowledgedResponse = {
    val request = indices.foldLeft(new IndicesAliasesRequest()) {
      case (request, idx) =>
        request.addAliasAction(AliasActions.add().index(idx).alias(alias))
    }

    if (removePrevious) {
      val getAliasesResponse =
        client.getAliases(new GetAliasesRequest(alias)).actionGet(timeout)
      val indexAliacesToRemove = getAliasesResponse.getAliases().keysIt().asScala
      Logger.info(s"Removing alias $alias from ${indexAliacesToRemove.mkString(", ")}")

      indexAliacesToRemove.foreach { indexName =>
        request.addAliasAction(AliasActions.remove().index(indexName).alias(alias))
      }
    }

    client.aliases(request.timeout(timeout)).get
  }

  /**
   * Add index alias with an option to remove the alias from all other indexes if it is already
   * pointed to any.
   * If index already exists or some other error occurs this results in a [[scala.util.Failure]].
   *
   * @param alias            to be re-assigned
   * @param index            index to point the alias to
   * @param removePrevious   When set to true, the indexAlias would be removed from all indices it
   *                         was assigned to before adding new index alias assignment.
   */
  def createOrUpdateAlias(
    esOptions: ElasticsearchOptions,
    alias: String,
    index: String,
    removePrevious: Boolean,
    timeout: TimeValue
  ): Try[AcknowledgedResponse] =
    createOrUpdateAlias(esOptions, List(index), alias, removePrevious, timeout)

  /**
   * Add or update index alias with an option to remove the alias from all other indexes if it is already
   * pointed to any.
   * If index already exists or some other error occurs this results in a [[scala.util.Failure]].
   *
   * @param alias            to be re-assigned
   * @param indices          indices to point the alias to
   * @param removePrevious   When set to true, the indexAlias would be removed from all indices it
   *                         was assigned to before adding new index alias assignment.
   */
  def createOrUpdateAlias(
    esOptions: ElasticsearchOptions,
    indices: Iterable[String],
    alias: String,
    removePrevious: Boolean,
    timeout: TimeValue
  ): Try[AcknowledgedResponse] =
    adminClient(esOptions) { client =>
      createOrUpdateAlias(client.indices(), indices, alias, removePrevious, timeout)
    }
}
