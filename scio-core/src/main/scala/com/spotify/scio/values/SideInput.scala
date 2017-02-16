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

import java.lang.{Iterable => JIterable}
import java.util.{List => JList, Map => JMap}

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.values.PCollectionView

import scala.collection.JavaConverters._

/** Encapsulate an SCollection when it is being used as a side input. */
trait SideInput[T] extends Serializable {

  private var cache: T = null.asInstanceOf[T]

  protected def get[I, O](context: DoFn[I, O]#ProcessContext): T

  private[values] def getCache[I, O](context: DoFn[I, O]#ProcessContext): T = {
    if (cache == null) {
      // this is called once per DoFn instance, which means once per CPU core on Dataflow
      cache = get(context)
    }
    cache
  }

  private[values] val view: PCollectionView[_]

}

private[values] class SingletonSideInput[T](val view: PCollectionView[T])
  extends SideInput[T] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): T = context.sideInput(view)
}

private[values] class ListSideInput[T](val view: PCollectionView[JList[T]])
  extends SideInput[List[T]] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): List[T] =
    context.sideInput(view).asScala.toList
}

private[values] class IterableSideInput[T](val view: PCollectionView[JIterable[T]])
  extends SideInput[Iterable[T]] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): Iterable[T] =
    context.sideInput(view).asScala
}

private[values] class MapSideInput[K, V](val view: PCollectionView[JMap[K, V]])
  extends SideInput[Map[K, V]] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): Map[K, V] =
    new SideInputMap(context.sideInput(view))
}

private[values] class MultiMapSideInput[K, V](val view: PCollectionView[JMap[K, JIterable[V]]])
  extends SideInput[Map[K, Iterable[V]]] {
  override def get[I, O](context: DoFn[I, O]#ProcessContext): Map[K, Iterable[V]] =
    new SideInputMultiMap(context.sideInput(view))
}

/** Encapsulate context of one or more [[SideInput]]s in an [[SCollectionWithSideInput]]. */
// TODO: scala 2.11
// class SideInputContext[T] private[scio] (private val context: DoFn[T, AnyRef]#ProcessContext)
//   extends AnyVal {
class SideInputContext[T] private[scio] (val context: DoFn[T, AnyRef]#ProcessContext) {
  /** Extract the value of a given [[SideInput]]. */
  def apply[S](side: SideInput[S]): S = side.getCache(context)
}

// Immutable wrapper for j.u.Map[A, B] because .asScala returns a s.c.mutable.Map
private class SideInputMap[A, B](self: JMap[A, B]) extends Map[A, B] {

  // make eager copies when necessary
  // scalastyle:off method.name
  override def +[B1 >: B](kv: (A, B1)): Map[A, B1] = self.asScala.toMap + kv
  override def -(key: A): Map[A, B] = self.asScala.toMap - key
  // scalastyle:on method.name

  // lazy transform underlying j.l.Map
  override def get(key: A): Option[B] = Option(self.get(key))
  override def iterator: Iterator[(A, B)] = self.asScala.iterator

}

// Immutable wrapper for j.u.Map[A, j.l.terable[B]] because .asScala returns a s.c.mutable.Map
private class SideInputMultiMap[A, B](self: JMap[A, JIterable[B]]) extends Map[A, Iterable[B]] {

  // make eager copies when necessary
  // scalastyle:off method.name
  override def +[B1 >: Iterable[B]](kv: (A, B1)): Map[A, B1] =
    self.asScala.mapValues(_.asScala).toMap + kv
  override def -(key: A): Map[A, Iterable[B]] = self.asScala.mapValues(_.asScala).toMap - key
  // scalastyle:on method.name

  // lazy transform underlying j.l.Map
  override def get(key: A): Option[Iterable[B]] = Option(self.get(key)).map(_.asScala)
  override def iterator: Iterator[(A, Iterable[B])] =
    self.asScala.iterator.map(kv => (kv._1, kv._2.asScala))

}
