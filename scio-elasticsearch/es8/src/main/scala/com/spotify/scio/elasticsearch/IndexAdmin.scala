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

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.Time
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping
import co.elastic.clients.elasticsearch.indices.update_aliases.{Action, RemoveAction}
import co.elastic.clients.elasticsearch.indices._
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client._
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.Try

object IndexAdmin {
  private[this] val Logger = LoggerFactory.getLogger(getClass)

  private def indicesClient[A](
    esOptions: ElasticsearchOptions
  )(f: ElasticsearchIndicesClient => A): Try[A] = {
    val provider = esOptions.usernameAndPassword.map { case (username, password) =>
      val credentials = new UsernamePasswordCredentials(username, password)
      val provider = new BasicCredentialsProvider()
      provider.setCredentials(AuthScope.ANY, credentials)
      provider
    }

    val builder = RestClient.builder(esOptions.nodes: _*)
    provider.foreach(p => builder.setHttpClientConfigCallback(_.setDefaultCredentialsProvider(p)))
    val restClient = builder.build()
    val transport = new RestClientTransport(restClient, new JacksonJsonpMapper())
    val client = new ElasticsearchClient(transport)
    val result = Try(f(client.indices()))
    transport.close()
    result
  }

  /**
   * Ensure that index is created.
   *
   * @param index
   *   index to be created
   * @param typeMappings
   */
  private def ensureIndex(
    index: String,
    typeMappings: TypeMapping,
    client: ElasticsearchIndicesClient
  ): CreateIndexResponse =
    client.create(CreateIndexRequest.of(_.index(index).mappings(typeMappings)))

  /**
   * Ensure that index is created. If index already exists or some other error occurs this results
   * in a [[scala.util.Failure]].
   *
   * @param index
   *   index to be created
   * @param typeMappings
   */
  def ensureIndex(
    esOptions: ElasticsearchOptions,
    index: String,
    typeMappings: TypeMapping
  ): Try[CreateIndexResponse] =
    indicesClient(esOptions)(client => ensureIndex(index, typeMappings, client))

  /**
   * Delete index
   *
   * @param index
   *   to be deleted
   * @param timeout
   *   defaults to 1 minute
   * @return
   *   Failure or unacknowledged response if operation did not succeed
   */
  private def removeIndex(
    client: ElasticsearchIndicesClient,
    index: String,
    timeout: Time
  ): DeleteIndexResponse =
    client.delete(DeleteIndexRequest.of(_.index(index).timeout(timeout)))

  /**
   * Delete index
   *
   * @param index
   *   to be deleted
   * @param timeout
   *   defaults to 1 minute
   * @return
   *   Failure or unacknowledged response if operation did not succeed
   */
  def removeIndex(
    esOptions: ElasticsearchOptions,
    index: String,
    timeout: Time
  ): Try[DeleteIndexResponse] =
    indicesClient(esOptions)(client => removeIndex(client, index, timeout))

  /**
   * Add or update index alias with an option to remove the alias from all other indexes if it is
   * already pointed to any.
   *
   * @param alias
   *   to be re-assigned
   * @param indices
   *   Iterable of pairs (index, isWriteIndex) to point the alias to. Note: only one index can be
   *   assigned as write index.
   * @param removePrevious
   *   When set to true, the indexAlias would be removed from all indices it was assigned to before
   *   adding new index alias assignment
   */
  private def createOrUpdateAlias(
    client: ElasticsearchIndicesClient,
    indices: Iterable[(String, Boolean)],
    alias: String,
    removePrevious: Boolean,
    timeout: Time
  ): UpdateAliasesResponse = {

    val (writeIdx, idxs) = indices.toList
      .partitionMap {
        case (idx, true)  => Left(idx)
        case (idx, false) => Right(idx)
      }

    require(
      writeIdx.size == 1,
      "Only one index per alias can be assigned to be the write index at a time"
    )

    val actionsBuilder = List.newBuilder[Action]
    actionsBuilder += Action.of(_.add(_.indices(writeIdx.asJava).isWriteIndex(true).alias(alias)))
    actionsBuilder += Action.of(_.add(_.indices(idxs.asJava).isWriteIndex(false).alias(alias)))

    if (removePrevious) {
      val getAliasesResponse = client.getAlias(GetAliasRequest.of(_.name(alias)))
      val indicesToRemove = getAliasesResponse.result().asScala.keys.toList
      Logger.info(s"Removing alias $alias from ${indicesToRemove.mkString(", ")}")

      actionsBuilder += Action.of(_.remove(_.indices(indicesToRemove.asJava).alias(alias)))
    }

    client.updateAliases(
      UpdateAliasesRequest.of(_.actions(actionsBuilder.result().asJava).timeout(timeout))
    )
  }

  /**
   * Add index alias with an option to remove the alias from all other indexes if it is already
   * pointed to any. If index already exists or some other error occurs this results in a
   * [[scala.util.Failure]].
   *
   * @param alias
   *   to be re-assigned
   * @param index
   *   index to point the alias to
   * @param removePrevious
   *   When set to true, the indexAlias would be removed from all indices it was assigned to before
   *   adding new index alias assignment.
   */
  def createOrUpdateAlias(
    esOptions: ElasticsearchOptions,
    alias: String,
    index: String,
    removePrevious: Boolean,
    timeout: Time
  ): Try[UpdateAliasesResponse] =
    createOrUpdateAlias(esOptions, List((index, true)), alias, removePrevious, timeout)

  /**
   * Add or update index alias with an option to remove the alias from all other indexes if it is
   * already pointed to any. If index already exists or some other error occurs this results in a
   * [[scala.util.Failure]].
   *
   * @param alias
   *   to be re-assigned
   * @param indices
   *   Iterable of pairs (index, isWriteIndex) to point the alias to. Note: only one index can be
   *   assigned as write index.
   * @param removePrevious
   *   When set to true, the indexAlias would be removed from all indices it was assigned to before
   *   adding new index alias assignment
   */
  def createOrUpdateAlias(
    esOptions: ElasticsearchOptions,
    indices: Iterable[(String, Boolean)],
    alias: String,
    removePrevious: Boolean,
    timeout: Time
  ): Try[UpdateAliasesResponse] =
    indicesClient(esOptions) { client =>
      createOrUpdateAlias(client, indices, alias, removePrevious, timeout)
    }

}
