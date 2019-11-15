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

import com.github.benmanes.caffeine.cache.{Caffeine, Cache => CCache}
import com.google.common.cache.{Cache => GCache, CacheBuilder => GCacheBuilder}
import java.util.function.{Function => JFunction}
import java.util.concurrent.{Callable => JCallable, ConcurrentHashMap => JConcurrentHashMap}

trait Cache[K, V] extends Serializable {
  type Underlying

  def underlying: Underlying
  def get(k: K): Option[V]
  def get(k: K, default: => V): V
  def put(k: K, value: V): Unit
  def invalidateAll(): Unit
}

trait CacheT[K, V, U] extends Cache[K, V] {
  override type Underlying = U
}

object Cache {
  def noOp[K, V]: CacheT[K, V, Nothing] = new CacheT[K, V, Nothing] {
    override def underlying: Nothing = ???
    override def get(k: K): Option[V] = None
    override def get(k: K, default: => V): V = default
    override def put(k: K, value: V): Unit = ()
    override def invalidateAll(): Unit = ()
  }

  def concurrentHashMap[K, V]: CacheT[K, V, JConcurrentHashMap[K, V]] =
    concurrentHashMap(new JConcurrentHashMap[K, V]())

  def concurrentHashMap[K, V](
    chm: JConcurrentHashMap[K, V]
  ): CacheT[K, V, JConcurrentHashMap[K, V]] =
    new CacheT[K, V, JConcurrentHashMap[K, V]] {
      override def underlying: JConcurrentHashMap[K, V] = chm

      override def get(k: K): Option[V] = Option(chm.get(k))

      override def get(k: K, default: => V): V =
        chm.computeIfAbsent(k, new JFunction[K, V] {
          override def apply(key: K): V = default
        })

      override def put(k: K, value: V): Unit = chm.put(k, value)

      override def invalidateAll(): Unit = chm.clear()
    }

  def caffeine[K <: AnyRef, V <: AnyRef]: CacheT[K, V, CCache[K, V]] =
    caffeine(Caffeine.newBuilder().build[K, V]())

  def caffeine[K, V](cache: CCache[K, V]): CacheT[K, V, CCache[K, V]] =
    new CacheT[K, V, CCache[K, V]] {
      override val underlying: CCache[K, V] = cache

      override def get(key: K): Option[V] =
        Option(underlying.getIfPresent(key))

      override def get(key: K, default: => V): V =
        underlying.get(key, new JFunction[K, V] {
          override def apply(key: K): V = default
        })

      override def put(key: K, value: V): Unit =
        underlying.put(key, value)

      override def invalidateAll(): Unit =
        underlying.invalidateAll()
    }

  def guava[K <: AnyRef, V <: AnyRef](): CacheT[K, V, GCache[K, V]] =
    guava(GCacheBuilder.newBuilder().build[K, V]())

  def guava[K, V](cache: GCache[K, V]): CacheT[K, V, GCache[K, V]] =
    new CacheT[K, V, GCache[K, V]] {
      override val underlying: GCache[K, V] = cache

      override def get(key: K): Option[V] =
        Option(underlying.getIfPresent(key))

      override def get(key: K, default: => V): V =
        underlying.get(key, new JCallable[V] {
          override def call(): V = default
        })

      override def put(key: K, value: V): Unit =
        underlying.put(key, value)

      override def invalidateAll(): Unit =
        underlying.invalidateAll()
    }
}
