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

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client._
import org.elasticsearch.client.indices.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType

import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.Try

object IndexAdmin {
  private[this] val Logger = LoggerFactory.getLogger(getClass)

  private def indicesClient[A](esOptions: ElasticsearchOptions)(f: IndicesClient => A): Try[A] = {
    val client = new RestHighLevelClient(RestClient.builder(esOptions.nodes: _*))

    val result = Try(f(client.indices()))
    client.close()
    result
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
  ): CreateIndexResponse =
    client.create(
      new CreateIndexRequest(index).source(mappingSource, XContentType.JSON),
      RequestOptions.DEFAULT
    )

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
    indicesClient(esOptions)(client => ensureIndex(index, mappingSource, client))

  /**
   * Delete index
   * @param index to be deleted
   * @param timeout defaults to 1 minute
   * @return Failure or unacknowledged response if operation did not succeed
   */
  private def removeIndex(
    client: IndicesClient,
    index: String,
    timeout: TimeValue
  ): AcknowledgedResponse =
    client.delete(new DeleteIndexRequest(index).timeout(timeout), RequestOptions.DEFAULT)

  /**
   * Delete index
   * @param index to be deleted
   * @param timeout defaults to 1 minute
   * @return Failure or unacknowledged response if operation did not succeed
   */
  def removeIndex(
    esOptions: ElasticsearchOptions,
    index: String,
    timeout: TimeValue
  ): Try[AcknowledgedResponse] =
    indicesClient(esOptions)(client => removeIndex(client, index, timeout))

  /**
   * Add or update index alias with an option to remove the alias from all other indexes if it is already
   * pointed to any.
   *
   * @param alias            to be re-assigned
   * @param indices          Iterable of pairs (index, isWriteIndex) to point the alias to.
   *                         Note: only one index can be assigned as write index.
   * @param removePrevious   When set to true, the indexAlias would be removed from all indices it
   *                         was assigned to before adding new index alias assignment
   */
  private def createOrUpdateAlias(
    client: IndicesClient,
    indices: Iterable[(String, Boolean)],
    alias: String,
    removePrevious: Boolean,
    timeout: TimeValue
  ): AcknowledgedResponse = {
    require(
      indices.find(_._2).size == 1,
      "Only one index per alias can be assigned to be the write index at a time"
    )

    val request = indices.foldLeft(new IndicesAliasesRequest()) {
      case (request, (idx, isWriteIndex)) =>
        request.addAliasAction(
          new AliasActions(AliasActions.Type.ADD)
            .index(idx)
            .writeIndex(isWriteIndex)
            .alias(alias)
        )
    }

    if (removePrevious) {
      val getAliasesResponse = client.getAlias(new GetAliasesRequest(alias), RequestOptions.DEFAULT)
      val indexAliacesToRemove = getAliasesResponse.getAliases.asScala.keys
      Logger.info(s"Removing alias $alias from ${indexAliacesToRemove.mkString(", ")}")

      indexAliacesToRemove.foreach { indexName =>
        request.addAliasAction(
          new AliasActions(AliasActions.Type.REMOVE).index(indexName).alias(alias)
        )
      }
    }
    client.updateAliases(request.timeout(timeout), RequestOptions.DEFAULT);
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
    createOrUpdateAlias(esOptions, List((index, true)), alias, removePrevious, timeout)

  /**
   * Add or update index alias with an option to remove the alias from all other indexes if it is already
   * pointed to any.
   * If index already exists or some other error occurs this results in a [[scala.util.Failure]].
   *
   * @param alias            to be re-assigned
   * @param indices          Iterable of pairs (index, isWriteIndex) to point the alias to.
   *                         Note: only one index can be assigned as write index.
   * @param removePrevious   When set to true, the indexAlias would be removed from all indices it
   *                         was assigned to before adding new index alias assignment
   */
  def createOrUpdateAlias(
    esOptions: ElasticsearchOptions,
    indices: Iterable[(String, Boolean)],
    alias: String,
    removePrevious: Boolean,
    timeout: TimeValue
  ): Try[AcknowledgedResponse] =
    indicesClient(esOptions) { client =>
      createOrUpdateAlias(client, indices, alias, removePrevious, timeout)
    }

}
