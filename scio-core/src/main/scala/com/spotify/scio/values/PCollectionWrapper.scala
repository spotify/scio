/*
 * Copyright 2016 Spotify AB.
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
import org.apache.beam.sdk.values.{PCollection, POutput}

private[values] trait PCollectionWrapper[T] extends TransformNameable {

  /** The [[org.apache.beam.sdk.values.PCollection PCollection]] being wrapped internally. */
  val internal: PCollection[T]

  /**
   * The [[ScioContext]] associated with this
   * [[org.apache.beam.sdk.values.PCollection PCollection]].
   */
  val context: ScioContext

  private[scio] def applyInternal[Output <: POutput](
    transform: PTransform[_ >: PCollection[T], Output]): Output =
    internal.apply(this.tfName, transform)

  protected def pApply[U](
    transform: PTransform[_ >: PCollection[T], PCollection[U]])
    : SCollection[U] = {
    val t =
      if (classOf[Combine.Globally[T, U]] isAssignableFrom transform.getClass) {
        // In case PCollection is windowed
        transform.asInstanceOf[Combine.Globally[T, U]].withoutDefaults()
      } else {
        transform
      }
    context.wrap(this.applyInternal(t))
  }

  private[scio] def parDo[U: Coder](fn: DoFn[T, U]): SCollection[U] =
    this
      .pApply(ParDo.of(fn))
      .setCoder(CoderMaterializer.beam(context, Coder[U]))
}
