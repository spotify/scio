package com.spotify.scio.cosmosdb.read

import org.apache.beam.sdk.annotations.Experimental
import org.apache.beam.sdk.annotations.Experimental.Kind
import org.apache.beam.sdk.io.Read
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PBegin, PCollection}
import org.bson.Document
import org.slf4j.LoggerFactory

/** A {@link PTransform} to read data from CosmosDB Core (SQL) API. */
@Experimental(Kind.SOURCE_SINK)
private[cosmosdb] case class CosmosDbRead(
  endpoint: String = null,
  key: String = null,
  database: String = null,
  container: String = null,
  query: String = null
) extends PTransform[PBegin, PCollection[Document]] {

  private val log = LoggerFactory.getLogger(classOf[CosmosDbRead])

  /** Create new ReadCosmos based into previous ReadCosmos, modifying the endpoint */
  def withCosmosEndpoint(endpoint: String): CosmosDbRead = this.copy(endpoint = endpoint)

  def withCosmosKey(key: String): CosmosDbRead = this.copy(key = key)

  def withDatabase(database: String): CosmosDbRead = this.copy(database = database)

  def withQuery(query: String): CosmosDbRead = this.copy(query = query)

  def withContainer(container: String): CosmosDbRead = this.copy(container = container)

  override def expand(input: PBegin): PCollection[Document] = {
    log.debug(s"Read CosmosDB with endpoint: $endpoint and query: $query")
    validate()

    // input.getPipeline.apply(Read.from(new CosmosSource(this)))
    input.apply(Read.from(new CosmosDbBoundedSource(this)))
  }

  private def validate(): Unit = {
    require(endpoint != null, "CosmosDB endpoint is required")
    require(key != null, "CosmosDB key is required")
    require(database != null, "CosmosDB database is required")
    require(container != null, "CosmosDB container is required")
    require(query != null, "CosmosDB query is required")
  }
}
