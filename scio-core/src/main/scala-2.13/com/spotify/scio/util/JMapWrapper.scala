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

package com.spotify.scio.util

import java.lang.{Iterable => JIterable}
import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._

/**
 * Immutable wrappers for [[java.util.Map]].
 * Java `Map`s are mutable and `.asJava` returns `mutable.Map[K, V]` which is inconsistent and not
 * idiomatic Scala. When wrapping Beam API, in many cases the underlying [[java.util.Map]] is
 * immutable in nature and it's safe to wrap them with this.
 */
private[scio] object JMapWrapper {
  def ofMultiMap[A, B](self: JMap[A, JIterable[B]]): Map[A, Iterable[B]] =
    new Map[A, Iterable[B]] {
      // make eager copies when necessary

      override def removed(key: A): Map[A, Iterable[B]] =
        self.asScala.iterator
          .filter { case (k, _) => k != key }
          .map { case (k, v) =>
            (k, v.asScala)
          }
          .toMap

      // lazy transform underlying j.u.Map
      override def get(key: A): Option[Iterable[B]] =
        Option(self.get(key)).map(_.asScala)
      override def iterator: Iterator[(A, Iterable[B])] =
        self.asScala.iterator.map(kv => (kv._1, kv._2.asScala))

      override def updated[V1 >: Iterable[B]](key: A, value: V1): Map[A, V1] =
        self.asScala.iterator
          .map { case (k, v) => (k, v.asScala) }
          .toMap
          .updated(key, value)
    }

  def of[K, V](self: JMap[K, V]): Map[K, V] =
    new Map[K, V] {
      // make eager copies when necessary

      override def removed(key: K): Map[K, V] =
        self.asScala.iterator.filter { case (k, _) => k != key }.toMap

      // lazy transform underlying j.u.Map
      override def get(key: K): Option[V] = Option(self.get(key))
      override def iterator: Iterator[(K, V)] = self.asScala.iterator

      override def updated[V1 >: V](key: K, value: V1): Map[K, V1] =
        self.asScala.toMap.updated(key, value)
    }
}
