/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.extra

import java.util.UUID

import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput}
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Main package for Annoy side input APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.extra.annoy._
 * }}}
 *
 * Two metrics are available, Angular and Euclidean.
 *
 * To save an `SCollection[(Int, Array[Float])]` to an Annoy file:
 *
 * {{{
 * val s = sc.parallelize(Seq( 1-> Array(1.2f, 3.4f), 2 -> Array(2.2f, 1.2f)))
 * }}}
 *
 * Save to a temporary location:
 * {{{
 * val s1 = s.asAnnoy(Angular, 40, 10)
 * }}}
 *
 * Save to a specific location:
 * {{{
 * val s1 = s.asAnnoy(Angular, 40, 10, "gs://<bucket>/<path>")
 * }}}
 *
 * `SCollection[AnnoyUri]` can be converted into a side input:
 * {{{
 * val s = sc.parallelize(Seq( 1-> Array(1.2f, 3.4f), 2 -> Array(2.2f, 1.2f)))
 * val side = s.asAnnoySideInput(metric, dimension, numTrees)
 * }}}
 *
 * There's syntactic sugar for saving an SCollection and converting it to a side input:
 * {{{
 * val s = sc
 *   .parallelize(Seq( 1-> Array(1.2f, 3.4f), 2 -> Array(2.2f, 1.2f)))
 *   .asAnnoySideInput(metric, dimension, numTrees)
 * }}}
 *
 * An existing Annoy file can be converted to a side input directly:
 * {{{
 * sc.annoySideInput(metric, dimension, numTrees, "gs://<bucket>/<path>")
 * }}}
 *
 * `AnnoyReader` provides nearest neighbor lookups by vector as well as item lookups:
 * {{{
 * val data = (0 until 1000).map(x => (x, Array.fill(40)(r.nextFloat())))
 * val main = sc.parallelize(data)
 * val side = main.asAnnoySideInput(metric, dimension, numTrees)
 *
 * main.keys.withSideInput(side)
 *   .map { (i, s) =>
 *     val annoyReader = s(side)
 *
 *     // get vector by item id, allocating a new Array[Float] each time
 *     val v1 = annoyReader.getItemVector(i)
 *
 *     // get vector by item id, copy vector into pre-allocated Array[Float]
 *     val v2 = Array.fill(dim)(-1.0f)
 *     annoyReader.getItemVector(i, v2)
 *
 *     // get 10 nearest neighbors by vector
 *     val results = annoyReader.getNearest(v2, 10)
 *   }
 * }}}
 */
package object annoy {

  sealed trait AnnoyMetric
  case object Angular extends AnnoyMetric
  case object Euclidean extends AnnoyMetric

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * AnnoyReader class for approximate nearest neighbor lookups.
   * Supports vector lookup by item  as well as nearest neighbor lookup by vector.
   *
   * @param path Can be either a local file or a GCS location e.g. gs://<bucket>/<path>
   * @param metric One of Angular (cosine distance) or Euclidean
   * @param dim Number of dimensions in vectors
   */
  class AnnoyReader private[annoy] (path: String, metric: AnnoyMetric, dim: Int) {

    require(dim > 0, "Vector dimension should be > 0")

    import com.spotify.annoy._

    private val index = {
      val indexType = metric match {
        case Angular   => IndexType.ANGULAR
        case Euclidean => IndexType.EUCLIDEAN
      }
      new ANNIndex(dim, path, indexType)
    }

    /**
     * Gets vector associated with item i.
     */
    def getItemVector(i: Int): Array[Float] = index.getItemVector(i)

    /**
     * Copies vector associated with item i into vector v.
     */
    def getItemVector(i: Int, v: Array[Float]): Unit = index.getItemVector(i, v)

    /**
     * Gets maxNumResults nearest neighbors for vector v.
     */
    def getNearest(v: Array[Float], maxNumResults: Int): Seq[Int] =
      index.getNearest(v, maxNumResults).asScala.asInstanceOf[Seq[Int]]

  }

  /** Enhanced version of [[ScioContext]] with Annoy methods. */
  implicit class AnnoyScioContext(private val self: ScioContext) extends AnyVal {

    /**
     * Create a SideInput of [[AnnoyReader]] from an [[AnnoyUri]] base path, to be used with
     * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]
     *
     * @param metric Metric (Angular, Euclidean) used to build the Annoy index
     * @param dim Number of dimensions in vectors used to build the Annoy index
     */
    def annoySideInput(path: String, metric: AnnoyMetric, dim: Int): SideInput[AnnoyReader] = {
      val uri = AnnoyUri(path, self.options)
      val view = self.parallelize(Seq(uri)).applyInternal(View.asSingleton())
      new AnnoySideInput(view, metric, dim)
    }
  }

  implicit class AnnoyPairSCollection(val self: SCollection[(Int, Array[Float])]) {

    /**
     * Write the key-value pairs of this SCollection as an Annoy file to a specific location,
     * building the trees in the index according to the parameters provided.
     *
     * @param path Can be either a local file or a GCS location e.g. gs://<bucket>/<path>
     * @param metric One of Angular (cosine distance) or Euclidean
     * @param dim Number of dimensions in vectors
     * @param nTrees Number of trees to build. More trees means more precision & bigger indices.
     *               If nTrees is set to -1, the trees will automatically be built in such a way
     *               that they will take at most 2x the memory of the vectors.
     * @return A singleton SCollection containing the [[AnnoyUri]] of the saved files
     */
    def asAnnoy(path: String, metric: AnnoyMetric, dim: Int, nTrees: Int): SCollection[AnnoyUri] = {
      val uri = AnnoyUri(path, self.context.options)
      require(!uri.exists, s"Annoy URI ${uri.path} already exists")

      self.transform { in =>
        in.groupBy(_ => ())
          .map {
            case (_, xs) =>
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
     * @param nTrees Number of trees to build. More trees means more precision & bigger indices.
     *               If nTrees is set to -1, the trees will automatically be built in such a way
     *               that they will take at most 2x the memory of the vectors.
     * @return A singleton SCollection containing the [[AnnoyUri]] of the saved files
     */
    def asAnnoy(metric: AnnoyMetric, dim: Int, nTrees: Int): SCollection[AnnoyUri] = {
      val uuid = UUID.randomUUID()
      val tempLocation = self.context.options.getTempLocation
      require(tempLocation != null, s"--tempLocation arg is required")
      val path = s"$tempLocation/annoy-build-$uuid"
      this.asAnnoy(path, metric, dim, nTrees)
    }

    /**
     * Write the key-value pairs of this SCollection as an Annoy file to a temporary location,
     * building the trees in the index according to the parameters provided, then load the
     * trees as a side input.
     *
     * @param metric One of Angular (cosine distance) or Euclidean
     * @param dim Number of dimensions in vectors
     * @param nTrees Number of trees to build. More trees means more precision & bigger indices.
     *               If nTrees is set to -1, the trees will automatically be built in such a way
     *               that they will take at most 2x the memory of the vectors.
     * @return SideInput[AnnoyReader]
     */
    def asAnnoySideInput(metric: AnnoyMetric, dim: Int, nTrees: Int): SideInput[AnnoyReader] =
      self.asAnnoy(metric, dim, nTrees).asAnnoySideInput(metric, dim)
  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Annoy methods
   */
  implicit class AnnoySCollection(private val self: SCollection[AnnoyUri]) extends AnyVal {

    /**
     * Load Annoy index stored at [[AnnoyUri]] in this
     * [[com.spotify.scio.values.SCollection SCollection]].
     * @param metric Metric (Angular, Euclidean) used to build the Annoy index
     * @param dim Number of dimensions in vectors used to build the Annoy index
     * @return SideInput[AnnoyReader]
     */
    def asAnnoySideInput(metric: AnnoyMetric, dim: Int): SideInput[AnnoyReader] = {
      val view = self.applyInternal(View.asSingleton())
      new AnnoySideInput(view, metric, dim)
    }
  }

  private class AnnoySideInput(val view: PCollectionView[AnnoyUri], metric: AnnoyMetric, dim: Int)
      extends SideInput[AnnoyReader] {
    override def get[I, O](context: DoFn[I, O]#ProcessContext): AnnoyReader =
      context.sideInput(view).getReader(metric, dim)
  }
}
