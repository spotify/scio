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

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollectionView

/** Encapsulate an SCollection when it is being used as a side input. */
trait SideInput[T] extends Serializable {
  type ViewType

  private[values] def view: PCollectionView[ViewType]

  private[values] def get[I, O](context: DoFn[I, O]#ProcessContext): T

  /**
   * Create a new [[SideInput]] by applying a function on the elements wrapped in this SideInput.
   */
  def map[B](f: T => B): SideInput[B] = new DelegatingSideInput[T, B](this, f)
}

/** Companion object of [[SideInput]]. */
object SideInput {
  final def apply[T](pv: PCollectionView[T]): SideInput[T] = new SideInput[T] {
    override type ViewType = T
    override private[values] val view: PCollectionView[T] = pv

    override private[values] def get[I, O](context: DoFn[I, O]#ProcessContext): T =
      context.sideInput(view)
  }
}

private[values] class DelegatingSideInput[A, B](val si: SideInput[A], val mapper: A => B)
    extends SideInput[B] {
  override type ViewType = si.ViewType

  override private[values] def get[I, O](context: DoFn[I, O]#ProcessContext): B =
    mapper(si.get(context))

  override private[values] val view: PCollectionView[ViewType] = si.view
}

/** Encapsulate context of one or more [[SideInput]]s in an [[SCollectionWithSideInput]]. */
class SideInputContext[T] private[scio] (val context: DoFn[T, AnyRef]#ProcessContext) {

  /** Extract the value of a given [[SideInput]]. */
  def apply[S](side: SideInput[S]): S = side.get(context)
}
