/*
 * Copyright 2019 Spotify AB.
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
package object annoy extends syntax.AllSyntax {

  @deprecated("Use syntax.AnnoyScioContextOps instead", "0.14.0")
  type AnnoyScioContext = syntax.AnnoyScioContextOps
  @deprecated("Use syntax.AnnoyPairSCollectionOps instead", "0.14.0")
  type AnnoyPairSCollection = syntax.AnnoyPairSCollectionOps
  @deprecated("Use syntax.AnnoySCollectionOps instead", "0.14.0")
  type AnnoySCollection = syntax.AnnoySCollectionOps
}
