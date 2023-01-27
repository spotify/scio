package com.spotify.scio.cosmosdb.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.cosmosdb.ReadCosmosDdIO
import com.spotify.scio.values.SCollection
import org.bson.Document


final class CosmosDbScioContextOps(private val sc: ScioContext) extends AnyVal {
  def cosmosDbCoreApi(): SCollection[Document] = {
    sc.read(ReadCosmosDdIO())
  }
}

trait ScioContextSyntax {
  implicit def cosmosdbScioContextOps(sc: ScioContext): CosmosDbScioContextOps = new CosmosDbScioContextOps(sc)
}
