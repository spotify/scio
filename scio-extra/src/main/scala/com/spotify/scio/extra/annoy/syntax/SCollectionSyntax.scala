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

package com.spotify.scio.extra.annoy.syntax

import com.spotify.scio.annotations.experimental
import com.spotify.scio.extra.annoy.Annoy.logger
import com.spotify.scio.extra.annoy.{
  AnnoyMetric,
  AnnoyReader,
  AnnoySideInput,
  AnnoyUri,
  AnnoyWriter
}
import com.spotify.scio.values.{SCollection, SideInput}
import org.apache.beam.sdk.transforms.View

import java.util.UUID

/** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Annoy methods */
class AnnoySCollectionOps(@transient private val self: SCollection[AnnoyUri]) extends AnyVal {

  /**
   * Load Annoy index stored at [[AnnoyUri]] in this
   * [[com.spotify.scio.values.SCollection SCollection]].
   * @param metric
   *   Metric (Angular, Euclidean) used to build the Annoy index
   * @param dim
   *   Number of dimensions in vectors used to build the Annoy index
   * @return
   *   SideInput[AnnoyReader]
   */
  @experimental
  def asAnnoySideInput(metric: AnnoyMetric, dim: Int): SideInput[AnnoyReader] = {
    val view = self.applyInternal(View.asSingleton[AnnoyUri]())
    new AnnoySideInput(view, metric, dim)
  }
}

class AnnoyPairSCollectionOps(@transient private val self: SCollection[(Int, Array[Float])])
    extends AnyVal {

  /**
   * Write the key-value pairs of this SCollection as an Annoy file to a specific location, building
   * the trees in the index according to the parameters provided.
   *
   * @param path
   *   Can be either a local file or a GCS location e.g. gs://<bucket>/<path>
   * @param metric
   *   One of Angular (cosine distance) or Euclidean
   * @param dim
   *   Number of dimensions in vectors
   * @param nTrees
   *   Number of trees to build. More trees means more precision & bigger indices. If nTrees is set
   *   to -1, the trees will automatically be built in such a way that they will take at most 2x the
   *   memory of the vectors.
   * @return
   *   A singleton SCollection containing the [[AnnoyUri]] of the saved files
   */
  @experimental
  def asAnnoy(path: String, metric: AnnoyMetric, dim: Int, nTrees: Int): SCollection[AnnoyUri] = {
    val uri = AnnoyUri(path, self.context.options)
    require(!uri.exists, s"Annoy URI ${uri.path} already exists")

    self.transform { in =>
      in.groupBy(_ => ())
        .map { case (_, xs) =>
          logger.info(s"Saving as Annoy: $uri")
          val startTime = System.nanoTime()
          val annoyWriter = new AnnoyWriter(metric, dim, nTrees)
          try {
            val it = xs.iterator
            while (it.hasNext) {
              val (k, v) = it.next()
              annoyWriter.addItem(k, v)
            }
            val size = annoyWriter.size
            uri.saveAndClose(annoyWriter)
            val elapsedTime = (System.nanoTime() - startTime) / 1000000000.0
            logger.info(s"Built index with $size items in $elapsedTime seconds")
          } catch {
            case e: Throwable =>
              annoyWriter.free()
              throw e
          }
          uri
        }
    }
  }

  /**
   * Write the key-value pairs of this SCollection as an Annoy file to a temporary location,
   * building the trees in the index according to the parameters provided.
   *
   * @param nTrees
   *   Number of trees to build. More trees means more precision & bigger indices. If nTrees is set
   *   to -1, the trees will automatically be built in such a way that they will take at most 2x the
   *   memory of the vectors.
   * @return
   *   A singleton SCollection containing the [[AnnoyUri]] of the saved files
   */
  @experimental
  def asAnnoy(metric: AnnoyMetric, dim: Int, nTrees: Int): SCollection[AnnoyUri] = {
    val uuid = UUID.randomUUID()
    val tempLocation = self.context.options.getTempLocation
    require(tempLocation != null, s"--tempLocation arg is required")
    val path = s"$tempLocation/annoy-build-$uuid"
    this.asAnnoy(path, metric, dim, nTrees)
  }

  /**
   * Write the key-value pairs of this SCollection as an Annoy file to a temporary location,
   * building the trees in the index according to the parameters provided, then load the trees as a
   * side input.
   *
   * @param metric
   *   One of Angular (cosine distance) or Euclidean
   * @param dim
   *   Number of dimensions in vectors
   * @param nTrees
   *   Number of trees to build. More trees means more precision & bigger indices. If nTrees is set
   *   to -1, the trees will automatically be built in such a way that they will take at most 2x the
   *   memory of the vectors.
   * @return
   *   SideInput[AnnoyReader]
   */
  @experimental
  def asAnnoySideInput(metric: AnnoyMetric, dim: Int, nTrees: Int): SideInput[AnnoyReader] =
    this.asAnnoy(metric, dim, nTrees).asAnnoySideInput(metric, dim)
}

trait SCollectionSyntax {
  implicit def annoySCollectionOps(self: SCollection[AnnoyUri]): AnnoySCollectionOps =
    new AnnoySCollectionOps(self)
  implicit def annoyPairSCollectionOps(
    self: SCollection[(Int, Array[Float])]
  ): AnnoyPairSCollectionOps =
    new AnnoyPairSCollectionOps(self)
}
