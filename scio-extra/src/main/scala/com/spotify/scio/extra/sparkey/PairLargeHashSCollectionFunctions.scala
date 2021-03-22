/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.extra.sparkey

import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.sparkey.instances.{SparkeyMap, SparkeySet}
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.sparkey.CompressionType

/**
 * Extra functions available on SCollections of (key, value) pairs for hash based joins
 * through an implicit conversion, using the Sparkey-backed LargeMapSideInput for dramatic speed
 * increases over the in-memory versions for datasets >100MB. As long as the RHS fits on disk,
 * these functions are usually much much faster than regular joins and save on shuffling.
 *
 * Note that these are nearly identical to the functions in PairHashSCollectionFunctions.scala,
 * but we can't reuse the implementations there as SideInput[T] is not covariant over T.
 *
 * @groupname join Join Operations
 */
class PairLargeHashSCollectionFunctions[K, V](private val self: SCollection[(K, V)]) {

  implicit private[this] val (keyCoder, valueCoder): (Coder[K], Coder[V]) =
    (self.keyCoder, self.valueCoder)

  /**
   * Perform an inner join by replicating `rhs` to all workers.
   * The right side should be <<10x smaller than the left side, and must fit on disk.
   *
   * @group join
   */
  def largeHashJoin[W](
    rhs: SCollection[(K, W)],
    numShards: Short = DefaultSideInputNumShards,
    compressionType: CompressionType = DefaultCompressionType,
    compressionBlockSize: Int = DefaultCompressionBlockSize
  ): SCollection[(K, (V, W))] = {
    implicit val wCoder: Coder[W] = rhs.valueCoder
    hashJoin(rhs.asLargeMultiMapSideInput(numShards, compressionType, compressionBlockSize))
  }

  /**
   * Perform an inner join with a MultiMap `SideInput[SparkeyMap[K, Iterable[V]]`
   *
   * The right side must fit on disk. The SideInput can be used reused for multiple joins.
   *
   * @example
   * {{{
   *   val si = pairSCollRight.asLargeMultiMapSideInput
   *   val joined1 = pairSColl1Left.hashJoin(si)
   *   val joined2 = pairSColl2Left.hashJoin(si)
   * }}}
   *
   * @group join
   */
  def hashJoin[W: Coder](
    sideInput: SideInput[SparkeyMap[K, Iterable[W]]]
  ): SCollection[(K, (V, W))] =
    self.transform { in =>
      in.withSideInputs(sideInput)
        .flatMap[(K, (V, W))] { (kv, sideInputCtx) =>
          sideInputCtx(sideInput)
            .getOrElse(kv._1, Iterable.empty[W])
            .iterator
            .map(w => (kv._1, (kv._2, w)))
        }
        .toSCollection
    }

  /**
   * Perform a left outer join by replicating `rhs` to all workers.
   * The right side must fit on disk.
   *
   * @example
   * {{{
   *   val si = pairSCollRight
   *   val joined = pairSColl1Left.largeHashLeftOuterJoin(pairSCollRight)
   * }}}
   * @group join
   * @param rhs The SCollection[(K, W)] treated as right side of the join.
   */
  def largeHashLeftOuterJoin[W](
    rhs: SCollection[(K, W)],
    numShards: Short = DefaultSideInputNumShards,
    compressionType: CompressionType = DefaultCompressionType,
    compressionBlockSize: Int = DefaultCompressionBlockSize
  ): SCollection[(K, (V, Option[W]))] = {
    implicit val wCoder: Coder[W] = rhs.valueCoder
    hashLeftOuterJoin(
      rhs.asLargeMultiMapSideInput(numShards, compressionType, compressionBlockSize)
    )
  }

  /**
   * Perform a left outer join with a MultiMap `SideInput[SparkeyMap[K, Iterable[V]]`
   *
   * @example
   * {{{
   *   val si = pairSCollRight.asLargeMultiMapSideInput
   *   val joined1 = pairSColl1Left.hashLeftOuterJoin(si)
   *   val joined2 = pairSColl2Left.hashLeftOuterJoin(si)
   * }}}
   * @group join
   */
  def hashLeftOuterJoin[W: Coder](
    sideInput: SideInput[SparkeyMap[K, Iterable[W]]]
  ): SCollection[(K, (V, Option[W]))] = {
    self.transform { in =>
      in.withSideInputs(sideInput)
        .flatMap[(K, (V, Option[W]))] { case ((k, v), sideInputCtx) =>
          // Using .get here instead of if/else to avoid calling .get twice on a disk-based map.
          sideInputCtx(sideInput)
            .get(k)
            .map(_.iterator.map(w => (k, (v, Some(w)))))
            .getOrElse(Iterator((k, (v, None))))
        }
        .toSCollection
    }
  }

  /**
   * Perform a full outer join by replicating `rhs` to all workers. The right side must fit on disk.
   *
   * @group join
   */
  def largeHashFullOuterJoin[W](
    rhs: SCollection[(K, W)],
    numShards: Short = DefaultSideInputNumShards,
    compressionType: CompressionType = DefaultCompressionType,
    compressionBlockSize: Int = DefaultCompressionBlockSize
  ): SCollection[(K, (Option[V], Option[W]))] = {
    implicit val wCoder = rhs.valueCoder
    hashFullOuterJoin(
      rhs.asLargeMultiMapSideInput(numShards, compressionType, compressionBlockSize)
    )
  }

  /**
   * Perform a full outer join with a `SideInput[SparkeyMap[K, Iterable[W]]]`.
   *
   * @example
   * {{{
   *   val si = pairSCollRight.asLargeMultiMapSideInput
   *   val joined1 = pairSColl1Left.hashFullOuterJoin(si)
   *   val joined2 = pairSColl2Left.hashFullOuterJoin(si)
   * }}}
   *
   * @group join
   */
  def hashFullOuterJoin[W: Coder](
    sideInput: SideInput[SparkeyMap[K, Iterable[W]]]
  ): SCollection[(K, (Option[V], Option[W]))] =
    self.transform { in =>
      val leftHashed = in
        .withSideInputs(sideInput)
        .flatMap { case ((k, v), sideInputCtx) =>
          val rhsSideMap = sideInputCtx(sideInput)
          if (rhsSideMap.contains(k)) {
            rhsSideMap(k).iterator
              .map[(K, (Option[V], Option[W]), Boolean)](w => (k, (Some(v), Some(w)), true))
          } else {
            Iterator((k, (Some(v), None), false))
          }
        }
        .toSCollection

      val rightHashed = leftHashed
        .filter(_._3)
        .map(_._1)
        .aggregate(Set.empty[K])(_ + _, _ ++ _)
        .withSideInputs(sideInput)
        .flatMap { (mk, sideInputCtx) =>
          val m = sideInputCtx(sideInput)
          (m.keySet diff mk)
            .flatMap(k => m(k).iterator.map[(K, (Option[V], Option[W]))](w => (k, (None, Some(w)))))
        }
        .toSCollection

      leftHashed.map(x => (x._1, x._2)) ++ rightHashed
    }

  /**
   * Return an SCollection with the pairs from `this` whose keys are in `rhs`
   * given `rhs` is small enough to fit on disk.
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @group per key
   */
  def largeHashIntersectByKey(
    rhs: SCollection[K],
    numShards: Short = DefaultSideInputNumShards,
    compressionType: CompressionType = DefaultCompressionType,
    compressionBlockSize: Int = DefaultCompressionBlockSize
  ): SCollection[(K, V)] =
    hashIntersectByKey(rhs.asLargeSetSideInput(numShards, compressionType, compressionBlockSize))

  /**
   * Return an SCollection with the pairs from `this` whose keys are in the SideSet `rhs`.
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @group per key
   */
  def hashIntersectByKey(sideInput: SideInput[SparkeySet[K]]): SCollection[(K, V)] =
    self
      .withSideInputs(sideInput)
      .filter { case ((k, _), sideInputCtx) => sideInputCtx(sideInput).contains(k) }
      .toSCollection

  /**
   * Return an SCollection with the pairs from `this` whose keys are not in SCollection[V] `rhs`.
   *
   * Rhs must be small enough to fit on disk.
   *
   * @group per key
   */
  def largeHashSubtractByKey(
    rhs: SCollection[K],
    numShards: Short = DefaultSideInputNumShards,
    compressionType: CompressionType = DefaultCompressionType,
    compressionBlockSize: Int = DefaultCompressionBlockSize
  ): SCollection[(K, V)] =
    hashSubtractByKey(rhs.asLargeSetSideInput(numShards, compressionType, compressionBlockSize))

  /**
   * Return an SCollection with the pairs from `this` whose keys are not in SideInput[Set] `rhs`.
   *
   * @group per key
   */
  def hashSubtractByKey(sideInput: SideInput[SparkeySet[K]]): SCollection[(K, V)] =
    self
      .withSideInputs(sideInput)
      .filter { case ((k, _), sideInputCtx) => !sideInputCtx(sideInput).contains(k) }
      .toSCollection

}
