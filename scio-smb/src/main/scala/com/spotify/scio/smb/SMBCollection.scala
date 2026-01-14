/*
 * Copyright 2025 Spotify AB
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

package com.spotify.scio.smb

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.{SCollection, SideInput, SideInputContext}
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, TargetParallelism}

/**
 * Internal key metadata tracked for SMB operations. Not exposed in public API - keys are either
 * embedded in values or extracted from reads.
 *
 * @param primaryClass
 *   The primary key class
 * @param secondaryClass
 *   The secondary key class (Void for primary-key-only)
 */
private[smb] case class SMBKeyMetadata(
  primaryClass: Class[_],
  secondaryClass: Class[_]
)

/**
 * Deferred value that materializes on first access.
 *
 * @tparam T
 *   The type of value being deferred
 */
trait Deferred[+T] {

  /** Materialize the deferred value. May trigger graph execution on first call. */
  def get: T

  /** Transform the deferred value without re-executing the graph. */
  def map[U](f: T => U): Deferred[U] = new Deferred[U] {
    def get: U = f(Deferred.this.get)
  }
}

/**
 * Fluent API for sorted-merge-bucket transformations with zero-shuffle multi-output.
 *
 * Simplified design matching SCollection semantics:
 *   - read: Returns `SMBCollection[K, Void, V]` (values only, keys tracked in type)
 *   - cogroup: Returns `SMBCollection[K, Void, (K, (V1, V2))]` (key in data)
 *
 * {{{
 * // Single read - values only
 * SMBCollection.read(usersRead)
 *   .map(user => transform(user))  // Only V in signature
 *   .saveAsSortedBucket(output)    // Type-safe: must match K1, K2
 *
 * // Cogroup - key is part of the data
 * val joined = SMBCollection.cogroup2(usersRead, accountsRead)
 *   // Type: SMBCollection[Integer, Void, (Integer, (Iterable[User], Iterable[Account]))]
 *
 * // Use keys via .values implicit
 * joined.values
 *   .map { case (users, accounts) => join(users, accounts) }
 *
 * // Multiple outputs with zero shuffles
 * val base = joined.map { case (k, (u, a)) => expensive(k, u, a) }
 * base.map(_.field1).saveAsSortedBucket(output1)
 * base.filter(_.total > 1000).saveAsSortedBucket(output2)
 *
 * sc.run()
 * }}}
 *
 * @tparam K1
 *   Primary key type (tracked for saveAsSortedBucket type safety)
 * @tparam K2
 *   Secondary key type (Void for primary-only)
 * @tparam V
 *   Value type
 */
@experimental
trait SMBCollection[K1, K2, V] {

  /** FlatMap over values. */
  def flatMap[W](f: V => TraversableOnce[W])(implicit coder: Coder[W]): SMBCollection[K1, K2, W]

  /** Transform values. */
  def map[W](f: V => W)(implicit coder: Coder[W]): SMBCollection[K1, K2, W] =
    flatMap(v => Some(f(v)))

  /** Filter values. */
  def filter(f: V => Boolean)(implicit coder: Coder[V]): SMBCollection[K1, K2, V] =
    flatMap(v => if (f(v)) Some(v) else None)

  /** Side-effecting operation for logging, metrics, etc. */
  def tap(f: V => Unit)(implicit coder: Coder[V]): SMBCollection[K1, K2, V] =
    flatMap { v => f(v); Some(v) }

  /**
   * Write this collection to SMB files. Executes automatically when `sc.run()` is called.
   *
   * Type-safe: output key types must match this collection's key types.
   *
   * @return
   *   Deferred ClosedTap that allows accessing written data after pipeline execution. Call `.get`
   *   on the returned Deferred AFTER `sc.run()` completes to access the tap.
   */
  def saveAsSortedBucket(output: SortedBucketIO.TransformOutput[K1, K2, V]): Deferred[ClosedTap[V]]

  /**
   * Convert to a deferred SCollection for further processing.
   *
   * Multiple deferred outputs from the same graph share execution:
   * {{{
   * val d1 = base.toDeferredSCollection
   * val d2 = base.map(_.field).toDeferredSCollection
   * val sc1 = d1.get  // Triggers execution
   * val sc2 = d2.get  // Reuses d1's execution
   * }}}
   */
  def toDeferredSCollection()(implicit sc: ScioContext, coder: Coder[V]): Deferred[SCollection[V]]

  /** Convert to SCollection immediately. Equivalent to `toDeferredSCollection().get`. */
  def toSCollectionAndSeal()(implicit sc: ScioContext, coder: Coder[V]): SCollection[V] =
    toDeferredSCollection().get

  /** Add side inputs for context-aware transformations. */
  def withSideInputs(sides: SideInput[_]*): SMBCollectionWithSideInputs[K1, K2, V]
}

/** SMBCollection with side inputs. All transformations receive SideInputContext. */
@experimental
trait SMBCollectionWithSideInputs[K1, K2, V] {
  def flatMap[W](f: (SideInputContext[_], V) => TraversableOnce[W])(implicit
    coder: Coder[W]
  ): SMBCollectionWithSideInputs[K1, K2, W]

  def map[W](f: (SideInputContext[_], V) => W)(implicit
    coder: Coder[W]
  ): SMBCollectionWithSideInputs[K1, K2, W] =
    flatMap((ctx, v) => Some(f(ctx, v)))

  def filter(f: (SideInputContext[_], V) => Boolean)(implicit
    coder: Coder[V]
  ): SMBCollectionWithSideInputs[K1, K2, V] =
    flatMap((ctx, v) => if (f(ctx, v)) Some(v) else None)

  def tap(f: (SideInputContext[_], V) => Unit)(implicit
    coder: Coder[V]
  ): SMBCollectionWithSideInputs[K1, K2, V] =
    flatMap { (ctx, v) => f(ctx, v); Some(v) }

  /**
   * Convert back to a regular SMBCollection, dropping the side input context requirement.
   *
   * After this conversion, subsequent operations won't need the SideInputContext parameter.
   *
   * Example:
   * {{{
   * val withSideInputs: SMBCollectionWithSideInputs[K, V] = base.withSideInputs(...)
   * val filtered = withSideInputs.filter((ctx, v) => ctx(sideInput).contains(v.id))
   *
   * // After filtering with side inputs, drop the context for cleaner operations
   * val result: SMBCollection[K, V] = filtered.toSMBCollection
   * result.map(v => transform(v))  // No context parameter needed
   * }}}
   */
  def toSMBCollection: SMBCollection[K1, K2, V]

  def saveAsSortedBucket(output: SortedBucketIO.TransformOutput[K1, K2, V]): Deferred[ClosedTap[V]]
  def toDeferredSCollection()(implicit sc: ScioContext, coder: Coder[V]): Deferred[SCollection[V]]
  def toSCollectionAndSeal()(implicit sc: ScioContext, coder: Coder[V]): SCollection[V] =
    toDeferredSCollection().get
}

object SMBCollection {

  /**
   * 2-way cogroup on SMB sources.
   *
   * Returns key-value pairs like SCollection, where the key is the SMB bucketing key:
   * {{{
   * val joined = SMBCollection.cogroup2(classOf[Integer], usersRead, accountsRead)
   *   // Type: SMBCollection[Integer, Void, (Integer, (Iterable[User], Iterable[Account]))]
   *
   * // Use the key
   * joined.filter { case (key, _) => key > 100 }
   *
   * // Drop the key (like SCollection.values)
   * joined.values.map { case (users, accounts) => join(users, accounts) }
   * }}}
   */
  def cogroup2[K1: Coder, L: Coder, R: Coder](
    keyClass: Class[K1],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K1, Void, (K1, (Iterable[L], Iterable[R]))] = {
    SMBCollectionImpl.validateKeyClasses(List(lhs, rhs), keyClass, None)

    SMBCollectionImpl.createRoot[K1, Void, (K1, (Iterable[L], Iterable[R]))](
      sources = List(lhs, rhs),
      targetParallelism = targetParallelism,
      fromResult = { (key1: Any, _: Any, values: List[Iterable[Any]]) =>
        (
          key1.asInstanceOf[K1],
          (
            values(0).asInstanceOf[Iterable[L]],
            values(1).asInstanceOf[Iterable[R]]
          )
        )
      },
      keyClass = keyClass,
      keyClassSecondary = classOf[Void]
    )
  }

  /**
   * 2-way cogroup on SMB sources with composite (primary + secondary) keys.
   *
   * Returns `((K1, K2), (V1, V2))` where K1 is primary key, K2 is secondary key.
   */
  def cogroup2WithSecondary[K1: Coder, K2: Coder, L: Coder, R: Coder](
    primaryClass: Class[K1],
    secondaryClass: Class[K2],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K1, K2, ((K1, K2), (Iterable[L], Iterable[R]))] = {
    SMBCollectionImpl.validateKeyClasses(List(lhs, rhs), primaryClass, Some(secondaryClass))

    SMBCollectionImpl.createRoot[K1, K2, ((K1, K2), (Iterable[L], Iterable[R]))](
      sources = List(lhs, rhs),
      targetParallelism = targetParallelism,
      fromResult = { (key1: Any, key2: Any, values: List[Iterable[Any]]) =>
        (
          (key1.asInstanceOf[K1], key2.asInstanceOf[K2]),
          (
            values(0).asInstanceOf[Iterable[L]],
            values(1).asInstanceOf[Iterable[R]]
          )
        )
      },
      keyClass = primaryClass,
      keyClassSecondary = secondaryClass
    )
  }

  /** 3-way cogroup on SMB sources. Returns key-value pairs. */
  def cogroup3[K1: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K1],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit
    sc: ScioContext
  ): SMBCollection[K1, Void, (K1, (Iterable[A], Iterable[B], Iterable[C]))] = {
    SMBCollectionImpl.validateKeyClasses(List(a, b, c), keyClass, None)

    SMBCollectionImpl.createRoot[K1, Void, (K1, (Iterable[A], Iterable[B], Iterable[C]))](
      sources = List(a, b, c),
      targetParallelism = targetParallelism,
      fromResult = { (key1: Any, _: Any, values: List[Iterable[Any]]) =>
        (
          key1.asInstanceOf[K1],
          (
            values(0).asInstanceOf[Iterable[A]],
            values(1).asInstanceOf[Iterable[B]],
            values(2).asInstanceOf[Iterable[C]]
          )
        )
      },
      keyClass = keyClass,
      keyClassSecondary = classOf[Void]
    )
  }

  /** 3-way cogroup with composite keys. */
  def cogroup3WithSecondary[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder](
    primaryClass: Class[K1],
    secondaryClass: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit
    sc: ScioContext
  ): SMBCollection[K1, K2, ((K1, K2), (Iterable[A], Iterable[B], Iterable[C]))] = {
    SMBCollectionImpl.validateKeyClasses(List(a, b, c), primaryClass, Some(secondaryClass))

    SMBCollectionImpl.createRoot[K1, K2, ((K1, K2), (Iterable[A], Iterable[B], Iterable[C]))](
      sources = List(a, b, c),
      targetParallelism = targetParallelism,
      fromResult = { (key1: Any, key2: Any, values: List[Iterable[Any]]) =>
        (
          (key1.asInstanceOf[K1], key2.asInstanceOf[K2]),
          (
            values(0).asInstanceOf[Iterable[A]],
            values(1).asInstanceOf[Iterable[B]],
            values(2).asInstanceOf[Iterable[C]]
          )
        )
      },
      keyClass = primaryClass,
      keyClassSecondary = secondaryClass
    )
  }

  /** 4-way cogroup on SMB sources. Returns key-value pairs. */
  def cogroup4[K1: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    keyClass: Class[K1],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit
    sc: ScioContext
  ): SMBCollection[K1, Void, (K1, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
    SMBCollectionImpl.validateKeyClasses(List(a, b, c, d), keyClass, None)

    SMBCollectionImpl
      .createRoot[K1, Void, (K1, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))](
        sources = List(a, b, c, d),
        targetParallelism = targetParallelism,
        fromResult = { (key1: Any, _: Any, values: List[Iterable[Any]]) =>
          (
            key1.asInstanceOf[K1],
            (
              values(0).asInstanceOf[Iterable[A]],
              values(1).asInstanceOf[Iterable[B]],
              values(2).asInstanceOf[Iterable[C]],
              values(3).asInstanceOf[Iterable[D]]
            )
          )
        },
        keyClass = keyClass,
        keyClassSecondary = classOf[Void]
      )
  }

  /** 4-way cogroup with composite keys. */
  def cogroup4WithSecondary[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    primaryClass: Class[K1],
    secondaryClass: Class[K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit
    sc: ScioContext
  ): SMBCollection[K1, K2, ((K1, K2), (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
    SMBCollectionImpl.validateKeyClasses(
      List(a, b, c, d),
      primaryClass,
      Some(secondaryClass)
    )

    SMBCollectionImpl
      .createRoot[K1, K2, ((K1, K2), (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))](
        sources = List(a, b, c, d),
        targetParallelism = targetParallelism,
        fromResult = { (key1: Any, key2: Any, values: List[Iterable[Any]]) =>
          (
            (key1.asInstanceOf[K1], key2.asInstanceOf[K2]),
            (
              values(0).asInstanceOf[Iterable[A]],
              values(1).asInstanceOf[Iterable[B]],
              values(2).asInstanceOf[Iterable[C]],
              values(3).asInstanceOf[Iterable[D]]
            )
          )
        },
        keyClass = primaryClass,
        keyClassSecondary = secondaryClass
      )
  }

  /**
   * Single-source read.
   *
   * Returns just the values (no key), since keys are embedded in the data:
   * {{{
   * SMBCollection.read(classOf[Integer], usersRead)
   *   // Type: SMBCollection[Integer, Void, User]
   *   .map(user => transform(user))
   *   .filter(user => user.getId > 100)  // Access key via field
   * }}}
   */
  def read[K1: Coder, V: Coder](
    keyClass: Class[K1],
    source: SortedBucketIO.Read[V],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K1, Void, Iterable[V]] = {
    SMBCollectionImpl.validateKeyClasses(List(source), keyClass, None)

    SMBCollectionImpl.createRoot[K1, Void, Iterable[V]](
      sources = List(source),
      targetParallelism = targetParallelism,
      fromResult = { (_: Any, _: Any, values: List[Iterable[Any]]) =>
        values(0).asInstanceOf[Iterable[V]]
      },
      keyClass = keyClass,
      keyClassSecondary = classOf[Void]
    )
  }

  /** Single-source read with composite keys. */
  def readWithSecondary[K1: Coder, K2: Coder, V: Coder](
    primaryClass: Class[K1],
    secondaryClass: Class[K2],
    source: SortedBucketIO.Read[V],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K1, K2, Iterable[V]] = {
    SMBCollectionImpl.validateKeyClasses(List(source), primaryClass, Some(secondaryClass))

    SMBCollectionImpl.createRoot[K1, K2, Iterable[V]](
      sources = List(source),
      targetParallelism = targetParallelism,
      fromResult = { (_: Any, _: Any, values: List[Iterable[Any]]) =>
        values(0).asInstanceOf[Iterable[V]]
      },
      keyClass = primaryClass,
      keyClassSecondary = secondaryClass
    )
  }
}
