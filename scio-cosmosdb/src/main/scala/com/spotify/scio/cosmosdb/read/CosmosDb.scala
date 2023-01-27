package com.spotify.scio.cosmosdb.read

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Read
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{ PBegin, PCollection }
import org.bson.Document

private[cosmosdb] object CosmosDb {
  def read(sc: ScioContext): SCollection[Document] = {
    sc.applyTransform(CosmosDbRead())
  }
}
