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

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.extra.annoy.{AnnoyMetric, AnnoyReader, AnnoySideInput, AnnoyUri}
import com.spotify.scio.values.SideInput
import org.apache.beam.sdk.transforms.View

/** Enhanced version of [[ScioContext]] with Annoy methods. */
class AnnoyScioContextOps(private val self: ScioContext) extends AnyVal {

  /**
   * Create a SideInput of [[AnnoyReader]] from an [[AnnoyUri]] base path, to be used with
   * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]
   *
   * @param metric
   *   Metric (Angular, Euclidean) used to build the Annoy index
   * @param dim
   *   Number of dimensions in vectors used to build the Annoy index
   */
  @experimental
  def annoySideInput(path: String, metric: AnnoyMetric, dim: Int): SideInput[AnnoyReader] = {
    val uri = AnnoyUri(path, self.options)
    val view = self.parallelize(Seq(uri)).applyInternal(View.asSingleton[AnnoyUri]())
    new AnnoySideInput(view, metric, dim)
  }
}

trait ScioContextSyntax {
  implicit def annoyScioContextOps(self: ScioContext): AnnoyScioContextOps =
    new AnnoyScioContextOps(self)
}
