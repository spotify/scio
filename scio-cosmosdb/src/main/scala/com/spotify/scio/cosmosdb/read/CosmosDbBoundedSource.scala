package com.spotify.scio.cosmosdb.read

import org.apache.beam.sdk.annotations.Experimental
import org.apache.beam.sdk.annotations.Experimental.Kind
import org.apache.beam.sdk.coders.{Coder, SerializableCoder}
import org.apache.beam.sdk.io.BoundedSource
import org.apache.beam.sdk.options.PipelineOptions
import org.bson.Document

import java.util
import java.util.Collections

/**
 * A CosmosDB Core (SQL) API {@link BoundedSource} reading {@link Document} from a given instance.
 */
@Experimental(Kind.SOURCE_SINK)
private[read] class CosmosDbBoundedSource(private[read] val readCosmos: CosmosDbRead)
    extends BoundedSource[Document] {

  /**
   * @inheritDoc
   *   TODO: You have to find a better way, maybe by partition key
   */
  override def split(
    desiredBundleSizeBytes: Long,
    options: PipelineOptions
  ): util.List[CosmosDbBoundedSource] =
    Collections.singletonList(this)

  /**
   * @inheritDoc
   *   The Cosmos DB Coro (SQL) API not support this metrics by the querys
   */
  override def getEstimatedSizeBytes(options: PipelineOptions) = 0L

  override def getOutputCoder: Coder[Document] = SerializableCoder.of(classOf[Document])

  override def createReader(options: PipelineOptions) =
    new CosmosDbBoundedReader(this)
}
