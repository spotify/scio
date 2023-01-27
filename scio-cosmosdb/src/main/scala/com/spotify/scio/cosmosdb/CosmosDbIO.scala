package com.spotify.scio.cosmosdb

import com.spotify.scio.ScioContext
import com.spotify.scio.cosmosdb.read.CosmosDbRead
import com.spotify.scio.io.*
import com.spotify.scio.values.SCollection
import org.bson.Document

trait CosmosDbIO[T] extends ScioIO[T] {}

case class ReadCosmosDdIO(
  endpoint: String = null,
  key: String = null,
  database: String = null,
  container: String = null,
  query: String = null
) extends CosmosDbIO[Document] {
  override type ReadP = Unit
  override type WriteP = Nothing
  override val tapT: TapT.Aux[Document, Nothing] = EmptyTapOf[Document]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Document] =
    sc.applyTransform(CosmosDbRead(endpoint, key, database, container, query))

  override protected def write(data: SCollection[Document], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("cosmosDbCoreApi is read-only")

  /**
   * Write options also return a `ClosedTap`. Once the job completes you can open the `Tap`. Tap
   * abstracts away the logic of reading the dataset directly as an Iterator[T] or re-opening it in
   * another ScioContext. The Future is complete once the job finishes. This can be used to do light
   * weight pipeline orchestration e.g. WordCountOrchestration.scala.
   */
  override def tap(read: ReadP): Tap[Nothing] = EmptyTap
}
