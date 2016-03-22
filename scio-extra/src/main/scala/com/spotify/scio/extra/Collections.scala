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

package com.spotify.scio.extra

import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.JavaConverters._

/**
 * Utilities for Scala collection library.
 *
 * Adds a `top` method to `Array[T]` and `Iterable[T]` and a `topByKey` method to `Array[(K, V)]`
 * and `Iterable[(K, V)]`.
 *
 * {{{
 * import com.spotify.scio.extra.Collections._
 *
 * val xs: Array[(String, Int)] = // ...
 * xs.top(5)(Ordering.by(_._2))
 * xs.topByKey(5)
 * }}}
 */
object Collections {

  private def topImpl[T](xs: Iterable[T], num: Int, ord: Ordering[T]): Iterable[T] = {
    require(num > 0, "num must be > 0")
    if (xs.isEmpty) {
      Iterable.empty[T]
    } else {
      val size = math.min(num, xs.size)
      MinMaxPriorityQueue
        .orderedBy(ord.reverse)
        .expectedSize(size)
        .maximumSize(size)
        .create[T](xs.asJava)
        .asScala
    }
  }

  private def topByKeyImpl[K, V](xs: Iterable[(K, V)], num: Int,
                                 ord: Ordering[V]): Map[K, Iterable[V]] = {
    require(num > 0, "num must be > 0")
    val size = math.min(num, xs.size)

    val m = scala.collection.mutable.Map[K, MinMaxPriorityQueue[V]]()
    xs.foreach { case (k, v) =>
      if (!m.contains(k)) {
        val pq = MinMaxPriorityQueue
          .orderedBy(ord.reverse)
          .expectedSize(size)
          .maximumSize(size)
          .create[V]()
        pq.add(v)
        m(k) = pq
      } else {
        m(k).add(v)
      }
    }
    m.mapValues(_.asScala).toMap
  }

  /** Enhance Array by adding a `top` method. */
  implicit class TopArray[T](self: Array[T]) {
    def top(num: Int)(implicit ord: Ordering[T]): Iterable[T] = topImpl(self, num, ord)
  }

  /** Enhance Iterable by adding a `top` method. */
  implicit class TopIterable[T](self: Iterable[T]) {
    def top(num: Int)(implicit ord: Ordering[T]): Iterable[T] = topImpl(self, num, ord)
  }

  /** Enhance Array by adding a `topByKey` method. */
  implicit class TopByKeyArray[K, V](self: Array[(K, V)]) {
    def topByKey(num: Int)(implicit ord: Ordering[V]): Map[K, Iterable[V]] =
      topByKeyImpl(self, num, ord)
  }

  /** Enhance Iterable by adding a `topByKey` method. */
  implicit class TopByKeyIterable[K, V](self: Iterable[(K, V)]) {
    def topByKey(num: Int)(implicit ord: Ordering[V]): Map[K, Iterable[V]] =
      topByKeyImpl(self, num, ord)
  }

}
