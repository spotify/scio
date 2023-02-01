/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.cosmosdb.read

import com.azure.cosmos.models.CosmosQueryRequestOptions
import com.azure.cosmos.{ CosmosClient, CosmosClientBuilder }
import com.spotify.scio.annotations.experimental
import org.apache.beam.sdk.io.BoundedSource
import org.bson.Document
import org.slf4j.LoggerFactory

@experimental
private[read] class CosmosDbBoundedReader(cosmosSource: CosmosDbBoundedSource)
    extends BoundedSource.BoundedReader[Document] {
  private val log = LoggerFactory.getLogger(getClass)
  private var maybeClient: Option[CosmosClient] = None
  private var maybeIterator: Option[java.util.Iterator[Document]] = None
  @volatile private var current: Option[Document] = None
  @volatile private var recordsReturned = 0L

  override def start(): Boolean = {
    maybeClient = Some(
      new CosmosClientBuilder().gatewayMode
        .endpointDiscoveryEnabled(false)
        .endpoint(cosmosSource.endpoint)
        .key(cosmosSource.key)
        .buildClient
    )

    maybeIterator = maybeClient.map { client =>
      log.info("Get the container name")

      log.info(s"Get the iterator of the query in container ${cosmosSource.container}")
      client
        .getDatabase(cosmosSource.database)
        .getContainer(cosmosSource.container)
        .queryItems(
          cosmosSource.query,
          new CosmosQueryRequestOptions(),
          classOf[Document]
        )
        .iterator()
    }

    advance()
  }

  override def advance(): Boolean = maybeIterator match {
    case Some(iterator) if iterator.hasNext =>
      current = Some(iterator.next())
      recordsReturned += 1
      true
    case _ =>
      false
  }

  override def getCurrent: Document = current.orNull

  override def getCurrentSource: CosmosDbBoundedSource = cosmosSource

  override def close(): Unit = {
    log.info("Closing reader after reading {} records.", recordsReturned)
    maybeClient.foreach(_.close())
    maybeClient = None
    maybeIterator = None
  }
}
