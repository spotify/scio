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

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.GlobalWindow

import com.spotify.scio.util.JMapWrapper

import scala.jdk.CollectionConverters._

/** Encapsulate an SCollection when it is being used as a side input. */
trait SideInput[T] extends Serializable {
  type ViewType

  private[values] def view: PCollectionView[ViewType]

  private[values] def get[I, O](context: DoFn[I, O]#ProcessContext): T

  @deprecated("will be removed; override get instead")
  private[values] def getCache[I, O](context: DoFn[I, O]#ProcessContext, window: BoundedWindow): T =
    get(context)

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

  /**
   * Wrap a view of a singleton as a [[SideInput]]. In most cases you want to use
   * [[SCollection.asSingletonSideInput(*]].
   *
   * @deprecated use SCollection.asSingletonSideInput or SideInput(View.asSingleton)
   */
  @deprecated("use SCollection.asSingletonSideInput or SideInput(View.asSingleton)", "0.9.1")
  def wrapSingleton[T](view: PCollectionView[T]): SideInput[T] =
    new SingletonSideInput[T](view)

  /**
   * Wrap a view of a [[java.util.List]] as a [[SideInput]]. In most cases you want to use
   * [[SCollection.asListSideInput]].
   *
   * @deprecated use SCollection.asListSideInput or SideInput(com.spotify.scio.values.View.asScalaList)
   */
  @deprecated(
    "use SCollection.asListSideInput or SideInput(com.spotify.scio.values.View.asScalaList)",
    "0.9.1"
  )
  def wrapList[T](view: PCollectionView[JList[T]]): SideInput[Seq[T]] =
    new ListSideInput[T](view)

  /**
   * Wrap a view of a [[java.lang.Iterable]] as a [[SideInput]]. In most cases you want to use
   * [[SCollection.asIterableSideInput]].
   *
   * @deprecated use SCollection.asIterableSideInput or SideInput(com.spotify.scio.values.View.asScalaIterable)
   */
  @deprecated(
    "use SCollection.asIterableSideInput or SideInput(com.spotify.scio.values.View.asScalaIterable)",
    "0.9.1"
  )
  def wrapIterable[T](view: PCollectionView[JIterable[T]]): SideInput[Iterable[T]] =
    new IterableSideInput[T](view)

  /**
   * Wrap a view of a [[java.util.Map]] as a [[SideInput]]. In most cases you want to use
   * [[PairSCollectionFunctions.asMapSideInput]].
   *
   * @deprecated use SCollection.asMapSideInput or SideInput(com.spotify.scio.values.View.asScalaMap)
   */
  @deprecated(
    "use SCollection.asMapSideInput or SideInput(com.spotify.scio.values.View.asScalaMap)",
    "0.9.1"
  )
  def wrapMap[K, V](view: PCollectionView[JMap[K, V]]): SideInput[Map[K, V]] =
    new MapSideInput[K, V](view)

  /**
   * Wrap a view of a multi-map as a [[SideInput]]. In most cases you want to use
   * [[PairSCollectionFunctions.asMultiMapSideInput]].
   *
   * @deprecated use SCollection.asMultiMapSideInput or SideInput(com.spotify.scio.values.View.asScalaMultimap)
   */
  @deprecated(
    "use SCollection.asMultiMapSideInput or SideInput(com.spotify.scio.values.View.asScalaMultimap)",
    "0.9.1"
  )
  def wrapMultiMap[K, V](
    view: PCollectionView[JMap[K, JIterable[V]]]
  ): SideInput[Map[K, Iterable[V]]] =
    new MultiMapSideInput[K, V](view)
}

private[values] class SingletonSideInput[T](val view: PCollectionView[T])
    extends CachedSideInput[T] {
  override type ViewType = T

  override def get[I, O](context: DoFn[I, O]#ProcessContext): T =
    context.sideInput(view)
}

private[values] class ListSideInput[T](val view: PCollectionView[JList[T]])
    extends CachedSideInput[Seq[T]] {
  override type ViewType = JList[T]

  override def get[I, O](context: DoFn[I, O]#ProcessContext): Seq[T] =
    context.sideInput(view).iterator().asScala.toSeq
}

private[values] class IterableSideInput[T](val view: PCollectionView[JIterable[T]])
    extends CachedSideInput[Iterable[T]] {
  override type ViewType = JIterable[T]

  override def get[I, O](context: DoFn[I, O]#ProcessContext): Iterable[T] =
    context.sideInput(view).asScala
}

private[values] class MapSideInput[K, V](val view: PCollectionView[JMap[K, V]])
    extends CachedSideInput[Map[K, V]] {
  override type ViewType = JMap[K, V]

  override def get[I, O](context: DoFn[I, O]#ProcessContext): Map[K, V] =
    JMapWrapper.of(context.sideInput(view))
}

private[values] class MultiMapSideInput[K, V](val view: PCollectionView[JMap[K, JIterable[V]]])
    extends CachedSideInput[Map[K, Iterable[V]]] {
  override type ViewType = JMap[K, JIterable[V]]

  override def get[I, O](context: DoFn[I, O]#ProcessContext): Map[K, Iterable[V]] =
    JMapWrapper.ofMultiMap(context.sideInput(view))

}

private[values] class DelegatingSideInput[A, B](val si: SideInput[A], val mapper: A => B)
    extends SideInput[B] {
  override type ViewType = si.ViewType

  override private[values] def get[I, O](context: DoFn[I, O]#ProcessContext): B =
    mapper(si.get(context))

  override private[values] val view: PCollectionView[ViewType] = si.view
}

@deprecated("will be removed; you should use SideInput")
private[values] trait CachedSideInput[T] extends SideInput[T] {
  private var cache: T = _
  @transient private var window: BoundedWindow = _

  // Use this attribute in implementations of SideInput to force caching
  // even on GlobalWindows. (Used to fix #1269)
  protected def updateCacheOnGlobalWindow = true

  override def getCache[I, O](context: DoFn[I, O]#ProcessContext, window: BoundedWindow): T = {
    if (
      cache == null || this.window != window ||
      (updateCacheOnGlobalWindow && window == GlobalWindow.INSTANCE)
    ) {
      this.window = window
      cache = get(context)
    }
    cache
  }

}

/** Encapsulate context of one or more [[SideInput]]s in an [[SCollectionWithSideInput]]. */
class SideInputContext[T] private[scio] (
  val context: DoFn[T, AnyRef]#ProcessContext,
  val window: BoundedWindow
) {

  /** Extract the value of a given [[SideInput]]. */
  def apply[S](side: SideInput[S]): S = side.getCache(context, window)
}
