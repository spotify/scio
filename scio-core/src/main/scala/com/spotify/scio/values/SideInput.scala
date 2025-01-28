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

import java.lang.{Iterable => JIterable}
import java.util.{List => JList, Map => JMap}

import com.spotify.scio.util.JMapWrapper
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, GlobalWindow}
import org.apache.beam.sdk.values.PCollectionView

import scala.jdk.CollectionConverters._

/** Encapsulate an SCollection when it is being used as a side input. */
trait SideInput[T] extends Serializable {
  private var cache: T = _
  @transient private var window: BoundedWindow = _

  // Use this attribute in implementations of SideInput to force caching
  // even on GlobalWindows. (Used to fix #1269)
  protected[values] def updateCacheOnGlobalWindow = true

  private[values] def get[I, O](context: DoFn[I, O]#ProcessContext): T

  def getCache[I, O](context: DoFn[I, O]#ProcessContext, window: BoundedWindow): T = {
    if (
      cache == null || this.window != window ||
      (updateCacheOnGlobalWindow && window == GlobalWindow.INSTANCE)
    ) {
      this.window = window
      cache = get(context)
    }
    cache
  }

  /** Create a new [[SideInput]] by applying a function on the elements wrapped in this SideInput. */
  @deprecated(since = "0.14.0")
  def map[B](f: T => B): SideInput[B] = new DelegatingSideInput[T, B](this, f)

  private[scio] val view: PCollectionView[_]
}

/** Companion object of [[SideInput]]. */
object SideInput {

  /**
   * Wrap a view of a singleton as a [[SideInput]]. In most cases you want to use
   * [[SCollection.asSingletonSideInput(*]].
   */
  def wrapSingleton[T](view: PCollectionView[T]): SideInput[T] =
    new SingletonSideInput[T](view)

  /**
   * Wrap a view of a [[java.util.List]] as a [[SideInput]]. In most cases you want to use
   * [[SCollection.asListSideInput]].
   */
  def wrapList[T](view: PCollectionView[JList[T]]): SideInput[Seq[T]] =
    new ListSideInput[T](view)

  /**
   * Wrap a view of a [[java.lang.Iterable]] as a [[SideInput]]. In most cases you want to use
   * [[SCollection.asIterableSideInput]].
   */
  def wrapIterable[T](view: PCollectionView[JIterable[T]]): SideInput[Iterable[T]] =
    new IterableSideInput[T](view)

  /**
   * Wrap a view of a [[java.util.Map]] as a [[SideInput]]. In most cases you want to use
   * [[PairSCollectionFunctions.asMapSideInput]].
   */
  def wrapMap[K, V](view: PCollectionView[JMap[K, V]]): SideInput[Map[K, V]] =
    new MapSideInput[K, V](view)

  /**
   * Wrap a view of a multi-map as a [[SideInput]]. In most cases you want to use
   * [[PairSCollectionFunctions.asMultiMapSideInput]].
   */
  def wrapMultiMap[K, V](
    view: PCollectionView[JMap[K, JIterable[V]]]
  ): SideInput[Map[K, Iterable[V]]] =
    new MultiMapSideInput[K, V](view)
}

private[values] class SingletonSideInput[T](val view: PCollectionView[T]) extends SideInput[T] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): T =
    context.sideInput(view)
}

private[values] class ListSideInput[T](val view: PCollectionView[JList[T]])
    extends SideInput[Seq[T]] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): Seq[T] =
    context.sideInput(view).iterator().asScala.toSeq
}

private[values] class IterableSideInput[T](val view: PCollectionView[JIterable[T]])
    extends SideInput[Iterable[T]] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): Iterable[T] =
    context.sideInput(view).asScala
}

private[values] class MapSideInput[K, V](val view: PCollectionView[JMap[K, V]])
    extends SideInput[Map[K, V]] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): Map[K, V] =
    JMapWrapper.of(context.sideInput(view))
}

private[values] class MultiMapSideInput[K, V](val view: PCollectionView[JMap[K, JIterable[V]]])
    extends SideInput[Map[K, Iterable[V]]] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): Map[K, Iterable[V]] =
    JMapWrapper.ofMultiMap(context.sideInput(view))
}

@deprecated(since = "0.14.0")
private[values] class DelegatingSideInput[A, B](val si: SideInput[A], val mapper: A => B)
    extends SideInput[B] {

  // Only update the cached value (and re-run the mapper) if the underlying SI does the same.
  override protected[values] def updateCacheOnGlobalWindow: Boolean = si.updateCacheOnGlobalWindow

  override def get[I, O](context: DoFn[I, O]#ProcessContext): B = mapper(si.get(context))

  private[scio] val view: PCollectionView[_] = si.view
}

/** Encapsulate context of one or more [[SideInput]]s in an [[SCollectionWithSideInput]]. */
class SideInputContext[T] private[scio] (
  val context: DoFn[T, AnyRef]#ProcessContext,
  val window: BoundedWindow
) {

  /** Extract the value of a given [[SideInput]]. */
  def apply[S](side: SideInput[S]): S = side.getCache(context, window)
}
