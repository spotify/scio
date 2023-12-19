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

package com.spotify.scio.extra.annoy

import com.spotify.scio.values.SideInput
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

object Annoy {
  @transient private[annoy] lazy val logger = LoggerFactory.getLogger(this.getClass)
}

sealed abstract class AnnoyMetric
case object Angular extends AnnoyMetric
case object Euclidean extends AnnoyMetric

/**
 * AnnoyReader class for approximate nearest neighbor lookups. Supports vector lookup by item as
 * well as nearest neighbor lookup by vector.
 *
 * @param path
 *   Can be either a local file or a GCS location e.g. gs://<bucket>/<path>
 * @param metric
 *   One of Angular (cosine distance) or Euclidean
 * @param dim
 *   Number of dimensions in vectors
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

  /** Gets vector associated with item i. */
  def getItemVector(i: Int): Array[Float] = index.getItemVector(i)

  /** Copies vector associated with item i into vector v. */
  def getItemVector(i: Int, v: Array[Float]): Unit = index.getItemVector(i, v)

  /** Gets maxNumResults nearest neighbors for vector v. */
  def getNearest(v: Array[Float], maxNumResults: Int): Seq[Int] =
    index.getNearest(v, maxNumResults).asScala.toSeq.asInstanceOf[Seq[Int]]
}

private[annoy] class AnnoySideInput(
  val view: PCollectionView[AnnoyUri],
  metric: AnnoyMetric,
  dim: Int
) extends SideInput[AnnoyReader] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): AnnoyReader =
    context.sideInput(view).getReader(metric, dim)
}
