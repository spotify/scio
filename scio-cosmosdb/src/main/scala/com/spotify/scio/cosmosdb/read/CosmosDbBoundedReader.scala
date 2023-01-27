package com.spotify.scio.cosmosdb.read

import com.azure.cosmos.models.CosmosQueryRequestOptions
import com.azure.cosmos.{CosmosClient, CosmosClientBuilder}
import org.apache.beam.sdk.annotations.Experimental
import org.apache.beam.sdk.annotations.Experimental.Kind
import org.apache.beam.sdk.io.BoundedSource
import org.bson.Document
import org.slf4j.LoggerFactory

@Experimental(Kind.SOURCE_SINK)
private[read] class CosmosDbBoundedReader(cosmosSource: CosmosDbBoundedSource)
    extends BoundedSource.BoundedReader[Document] {
  private val log = LoggerFactory.getLogger(getClass)
  private var maybeClient: Option[CosmosClient] = None
  private var maybeIterator: Option[java.util.Iterator[Document]] = None

  override def start(): Boolean = {
    maybeClient = Some(
      new CosmosClientBuilder().gatewayMode
        .endpointDiscoveryEnabled(false)
        .endpoint(cosmosSource.readCosmos.endpoint)
        .key(cosmosSource.readCosmos.key)
        .buildClient
    )

    maybeIterator = maybeClient.map { client =>
      log.info("Get the container name")

      log.info(s"Get the iterator of the query in container ${cosmosSource.readCosmos.container}")
      client
        .getDatabase(cosmosSource.readCosmos.database)
        .getContainer(cosmosSource.readCosmos.container)
        .queryItems(
          cosmosSource.readCosmos.query,
          new CosmosQueryRequestOptions(),
          classOf[Document]
        )
        .iterator()
    }

    true
  }

  override def advance(): Boolean = maybeIterator.exists(_.hasNext)

  override def getCurrent: Document =
    maybeIterator
      .filter(_.hasNext)
      // .map(iterator => new Document(iterator.next()))
      .map(_.next())
      .orNull

  override def getCurrentSource: CosmosDbBoundedSource = cosmosSource

  override def close(): Unit = maybeClient.foreach(_.close())
}
