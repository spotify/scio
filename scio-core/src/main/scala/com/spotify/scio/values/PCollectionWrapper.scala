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

package com.spotify.scio.values

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.transforms.{Combine, DoFn, PTransform, ParDo}
import org.apache.beam.sdk.values.{PCollection, POutput, WindowingStrategy}

private[values] trait PCollectionWrapper[T] extends TransformNameable {

  /** The [[org.apache.beam.sdk.values.PCollection PCollection]] being wrapped internally. */
  val internal: PCollection[T]

  implicit def coder: Coder[T] = Coder.beam(internal.getCoder)

  /**
   * The [[ScioContext]] associated with this
   * [[org.apache.beam.sdk.values.PCollection PCollection]].
   */
  val context: ScioContext

  private[scio] def applyInternal[Output <: POutput](
    name: Option[String],
    root: PTransform[_ >: PCollection[T], Output]
  ): Output =
    internal.apply(this.tfName(name), root)

  private[scio] def applyInternal[Output <: POutput](
    root: PTransform[_ >: PCollection[T], Output]
  ): Output =
    applyInternal(None, root)

  private[scio] def applyInternal[Output <: POutput](
    name: String,
    root: PTransform[_ >: PCollection[T], Output]
  ): Output =
    applyInternal(Option(name), root)
}
