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
 * Represents a key specification for SMB operations. Always has both primary (K1) and secondary
 * (K2) key types. For primary-key-only operations, K2 is Void.
 *
 * @param primaryClass
 *   The primary key class
 * @param secondaryClass
 *   The secondary key class (Void for primary-key-only)
 * @tparam K1
 *   The primary key type
 * @tparam K2
 *   The secondary key type (Void for primary-key-only)
 */
case class SMBKey[K1, K2](
  primaryClass: Class[K1],
  secondaryClass: Class[K2]
)

object SMBKey {

  /** Create a primary-key-only specification (K2 = Void). */
  def primary[K](implicit ct: scala.reflect.ClassTag[K]): SMBKey[K, Void] =
    SMBKey(ct.runtimeClass.asInstanceOf[Class[K]], classOf[Void])

  /** Create a composite (primary + secondary) key specification. */
  def composite[K1, K2](implicit
    ct1: scala.reflect.ClassTag[K1],
    ct2: scala.reflect.ClassTag[K2]
  ): SMBKey[K1, K2] =
    SMBKey(
      ct1.runtimeClass.asInstanceOf[Class[K1]],
      ct2.runtimeClass.asInstanceOf[Class[K2]]
    )
}

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
 * Enables shared computation across multiple outputs without GroupByKey operations:
 * {{{
 * val base = SMBCollection.cogroup2(classOf[String], users, events)
 *   .mapValues { case (users, events) =>
 *     expensiveComputation(users, events)  // Runs once per key
 *   }
 *
 * // Multiple outputs with zero shuffles
 * base.values.map(_.field1).saveAsSortedBucket(output1)
 * base.values.filter(_.total > 1000).saveAsSortedBucket(output2)
 *
 * sc.run()
 * }}}
 *
 * @tparam K1
 *   Primary key type for bucketing
 * @tparam K2
 *   Secondary key type (use Void for primary-key-only)
 * @tparam V
 *   Value type
 */
@experimental
trait SMBCollection[K1, K2, V] {

  /** FlatMap with access to keys and value. For primary keys, K2 will be Void. */
  def flatMap[W](f: (K1, K2, V) => TraversableOnce[W])(implicit
    coder: Coder[W]
  ): SMBCollection[K1, K2, W]

  /** Transform values while preserving keys. */
  def mapValues[W](f: V => W)(implicit coder: Coder[W]): SMBCollection[K1, K2, W] =
    flatMap((_, _, v) => Some(f(v)))

  /** Filter key-value pairs. */
  def filter(f: (K1, K2, V) => Boolean)(implicit coder: Coder[V]): SMBCollection[K1, K2, V] =
    flatMap((k1, k2, v) => if (f(k1, k2, v)) Some(v) else None)

  /** FlatMap values (0-N outputs per input). */
  def flatMapValues[W](f: V => TraversableOnce[W])(implicit
    coder: Coder[W]
  ): SMBCollection[K1, K2, W] =
    flatMap((_, _, v) => f(v))

  /** Side-effecting operation for logging, metrics, etc. */
  def tap(f: (K1, K2, V) => Unit)(implicit coder: Coder[V]): SMBCollection[K1, K2, V] =
    flatMap { (k1, k2, v) => f(k1, k2, v); Some(v) }

  /**
   * Value-only view where transformations don't expose keys. Reduces boilerplate:
   * `.values.filter(u => u.isActive)` vs `.filter((_, _, u) => u.isActive)`.
   */
  def values: SMBCollectionValues[K1, K2, V]

  /**
   * Write this collection to SMB files. Executes automatically when `sc.run()` is called.
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
   * val d2 = base.values.map(_.field).toDeferredSCollection
   * val sc1 = d1.get  // Triggers execution
   * val sc2 = d2.get  // Reuses d1's execution
   * }}}
   */
  def toDeferredSCollection()(implicit
    sc: ScioContext,
    coder: Coder[V]
  ): Deferred[SCollection[((K1, K2), V)]]

  /** Convert to SCollection immediately. Equivalent to `toDeferredSCollection().get`. */
  def toSCollectionAndSeal()(implicit
    sc: ScioContext,
    coder: Coder[V]
  ): SCollection[((K1, K2), V)] =
    toDeferredSCollection().get

  /** Add side inputs for context-aware transformations. */
  def withSideInputs(sides: SideInput[_]*): SMBCollectionWithSideInputs[K1, K2, V]
}

/** Value-only view of SMBCollection. Keys tracked internally for bucketing. */
@experimental
trait SMBCollectionValues[K1, K2, V] {
  def flatMap[W](f: V => TraversableOnce[W])(implicit
    coder: Coder[W]
  ): SMBCollectionValues[K1, K2, W]
  def map[W](f: V => W)(implicit coder: Coder[W]): SMBCollectionValues[K1, K2, W] =
    flatMap(v => Some(f(v)))
  def filter(f: V => Boolean)(implicit coder: Coder[V]): SMBCollectionValues[K1, K2, V] =
    flatMap(v => Some(v).filter(f))
  def tap(f: V => Unit)(implicit coder: Coder[V]): SMBCollectionValues[K1, K2, V] =
    flatMap { v => f(v); Some(v) }

  def saveAsSortedBucket(output: SortedBucketIO.TransformOutput[K1, K2, V]): Deferred[ClosedTap[V]]
  def keyed: SMBCollection[K1, K2, V]
  def toDeferredSCollection()(implicit sc: ScioContext, coder: Coder[V]): Deferred[SCollection[V]]
  def toSCollectionAndSeal()(implicit sc: ScioContext, coder: Coder[V]): SCollection[V] =
    toDeferredSCollection().get
}

/** SMBCollection with side inputs. All transformations receive SideInputContext. */
@experimental
trait SMBCollectionWithSideInputs[K1, K2, V] {
  def flatMap[W](f: (SideInputContext[_], K1, K2, V) => TraversableOnce[W])(implicit
    coder: Coder[W]
  ): SMBCollectionWithSideInputs[K1, K2, W]

  def mapValues[W](f: (SideInputContext[_], V) => W)(implicit
    coder: Coder[W]
  ): SMBCollectionWithSideInputs[K1, K2, W] =
    flatMap((ctx, _, _, v) => Some(f(ctx, v)))

  def filter(f: (SideInputContext[_], K1, K2, V) => Boolean)(implicit
    coder: Coder[V]
  ): SMBCollectionWithSideInputs[K1, K2, V] =
    flatMap((ctx, k1, k2, v) => if (f(ctx, k1, k2, v)) Some(v) else None)

  def flatMapValues[W](f: (SideInputContext[_], V) => TraversableOnce[W])(implicit
    coder: Coder[W]
  ): SMBCollectionWithSideInputs[K1, K2, W] =
    flatMap((ctx, _, _, v) => f(ctx, v))

  def tap(f: (SideInputContext[_], K1, K2, V) => Unit)(implicit
    coder: Coder[V]
  ): SMBCollectionWithSideInputs[K1, K2, V] =
    flatMap { (ctx, k1, k2, v) => f(ctx, k1, k2, v); Some(v) }

  def values: SMBCollectionWithSideInputsValues[K1, K2, V]
  def saveAsSortedBucket(output: SortedBucketIO.TransformOutput[K1, K2, V]): Deferred[ClosedTap[V]]
  def toDeferredSCollection()(implicit
    sc: ScioContext,
    coder: Coder[V]
  ): Deferred[SCollection[((K1, K2), V)]]
  def toSCollectionAndSeal()(implicit
    sc: ScioContext,
    coder: Coder[V]
  ): SCollection[((K1, K2), V)] =
    toDeferredSCollection().get
}

/** Value-only view with side inputs. All transformations receive SideInputContext. */
@experimental
trait SMBCollectionWithSideInputsValues[K1, K2, V] {
  def flatMap[W](f: (SideInputContext[_], V) => TraversableOnce[W])(implicit
    coder: Coder[W]
  ): SMBCollectionWithSideInputsValues[K1, K2, W]

  def map[W](f: (SideInputContext[_], V) => W)(implicit
    coder: Coder[W]
  ): SMBCollectionWithSideInputsValues[K1, K2, W] =
    flatMap((ctx, v) => Some(f(ctx, v)))

  def filter(f: (SideInputContext[_], V) => Boolean)(implicit
    coder: Coder[V]
  ): SMBCollectionWithSideInputsValues[K1, K2, V] =
    flatMap((ctx, v) => if (f(ctx, v)) Some(v) else None)

  def tap(f: (SideInputContext[_], V) => Unit)(implicit
    coder: Coder[V]
  ): SMBCollectionWithSideInputsValues[K1, K2, V] =
    flatMap { (ctx, v) => f(ctx, v); Some(v) }

  def keyed: SMBCollectionWithSideInputs[K1, K2, V]
  def saveAsSortedBucket(output: SortedBucketIO.TransformOutput[K1, K2, V]): Deferred[ClosedTap[V]]
  def toDeferredSCollection()(implicit sc: ScioContext, coder: Coder[V]): Deferred[SCollection[V]]
  def toSCollectionAndSeal()(implicit sc: ScioContext, coder: Coder[V]): SCollection[V] =
    toDeferredSCollection().get
}

object SMBCollection {

  /** 2-way cogroup on SMB sources. */
  def cogroup2[K1: Coder, K2: Coder, L: Coder, R: Coder](
    key: SMBKey[K1, K2],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K1, K2, (Iterable[L], Iterable[R])] = {
    implicit val valueCoder: Coder[(Iterable[L], Iterable[R])] =
      Coder.tuple2Coder(Coder.iterableCoder[L], Coder.iterableCoder[R])

    SMBCollectionImpl.validateKeyClasses(List(lhs, rhs), key.primaryClass, Some(key.secondaryClass))

    SMBCollectionImpl.createRoot[K1, K2, (Iterable[L], Iterable[R])](
      sources = List(lhs, rhs),
      targetParallelism = targetParallelism,
      fromResult = { values: List[Iterable[Any]] =>
        // Keep lazy - no materialization
        (
          values(0).asInstanceOf[Iterable[L]],
          values(1).asInstanceOf[Iterable[R]]
        )
      },
      keyClass = key.primaryClass,
      keyClassSecondary = key.secondaryClass
    )
  }

  /** 3-way cogroup on SMB sources. */
  def cogroup3[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder](
    key: SMBKey[K1, K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K1, K2, (Iterable[A], Iterable[B], Iterable[C])] = {
    implicit val valueCoder: Coder[(Iterable[A], Iterable[B], Iterable[C])] =
      Coder.tuple3Coder(Coder.iterableCoder[A], Coder.iterableCoder[B], Coder.iterableCoder[C])

    SMBCollectionImpl.validateKeyClasses(List(a, b, c), key.primaryClass, Some(key.secondaryClass))

    SMBCollectionImpl.createRoot[K1, K2, (Iterable[A], Iterable[B], Iterable[C])](
      sources = List(a, b, c),
      targetParallelism = targetParallelism,
      fromResult = { values: List[Iterable[Any]] =>
        // Keep lazy - no materialization
        (
          values(0).asInstanceOf[Iterable[A]],
          values(1).asInstanceOf[Iterable[B]],
          values(2).asInstanceOf[Iterable[C]]
        )
      },
      keyClass = key.primaryClass,
      keyClassSecondary = key.secondaryClass
    )
  }

  /** 4-way cogroup on SMB sources. */
  def cogroup4[K1: Coder, K2: Coder, A: Coder, B: Coder, C: Coder, D: Coder](
    key: SMBKey[K1, K2],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C],
    d: SortedBucketIO.Read[D],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit
    sc: ScioContext
  ): SMBCollection[K1, K2, (Iterable[A], Iterable[B], Iterable[C], Iterable[D])] = {
    implicit val valueCoder: Coder[(Iterable[A], Iterable[B], Iterable[C], Iterable[D])] =
      Coder.tuple4Coder(
        Coder.iterableCoder[A],
        Coder.iterableCoder[B],
        Coder.iterableCoder[C],
        Coder.iterableCoder[D]
      )

    SMBCollectionImpl.validateKeyClasses(
      List(a, b, c, d),
      key.primaryClass,
      Some(key.secondaryClass)
    )

    SMBCollectionImpl.createRoot[K1, K2, (Iterable[A], Iterable[B], Iterable[C], Iterable[D])](
      sources = List(a, b, c, d),
      targetParallelism = targetParallelism,
      fromResult = { values: List[Iterable[Any]] =>
        // Keep lazy - no materialization
        (
          values(0).asInstanceOf[Iterable[A]],
          values(1).asInstanceOf[Iterable[B]],
          values(2).asInstanceOf[Iterable[C]],
          values(3).asInstanceOf[Iterable[D]]
        )
      },
      keyClass = key.primaryClass,
      keyClassSecondary = key.secondaryClass
    )
  }

  /** Single-source read grouped by key. */
  def read[K1: Coder, K2: Coder, V: Coder](
    key: SMBKey[K1, K2],
    source: SortedBucketIO.Read[V],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K1, K2, Iterable[V]] = {
    implicit val valueCoder: Coder[Iterable[V]] = Coder.iterableCoder[V]

    SMBCollectionImpl.validateKeyClasses(List(source), key.primaryClass, Some(key.secondaryClass))

    SMBCollectionImpl.createRoot[K1, K2, Iterable[V]](
      sources = List(source),
      targetParallelism = targetParallelism,
      fromResult = { values: List[Iterable[Any]] =>
        // Keep lazy - no materialization
        values(0).asInstanceOf[Iterable[V]]
      },
      keyClass = key.primaryClass,
      keyClassSecondary = key.secondaryClass
    )
  }
}
