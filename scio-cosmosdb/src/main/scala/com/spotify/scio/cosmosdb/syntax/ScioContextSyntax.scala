package com.spotify.scio.cosmosdb.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.cosmosdb.ReadCosmosDdIO
import com.spotify.scio.values.SCollection
import org.bson.Document

trait ScioContextSyntax {
  implicit def cosmosdbScioContextOps(sc: ScioContext): CosmosDbScioContextOps =
    new CosmosDbScioContextOps(sc)
}

final class CosmosDbScioContextOps(private val sc: ScioContext) extends AnyVal {
  def readCosmosDbCoreApi(
    endpoint: String = null,
    key: String = null,
    database: String = null,
    container: String = null,
    query: String = null
  ): SCollection[Document] =
    sc.read(ReadCosmosDdIO(endpoint, key, database, container, query))
}
