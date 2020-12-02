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

import com.spotify.scio.coders.{BeamCoders, Coder}

/**
 * Extra functions available on SCollections of (key, value) pairs for hash based joins
 * through an implicit conversion.
 *
 * @groupname join Join Operations
 */
class PairHashSCollectionFunctions[K, V](val self: SCollection[(K, V)]) {

  implicit private[this] val (keyCoder: Coder[K], valueCoder: Coder[V]) =
    (self.keyCoder, self.valueCoder)

  /**
   * Perform an inner join by replicating `rhs` to all workers. The right side should be tiny and
   * fit in memory.
   *
   * @group join
   */
  def hashJoin[W](
    rhs: SCollection[(K, W)]
  ): SCollection[(K, (V, W))] =
    hashJoin(rhs.asMultiMapSingletonSideInput)

  /**
   * Perform an inner join with a MultiMap `SideInput[Map[K, Iterable[V]]`
   *
   * The right side is tiny and fits in memory. The SideInput can be used reused for
   * multiple joins.
   *
   * @example
   * {{{
   *   val si = pairSCollRight.asMultiMapSingletonSideInput
   *   val joined1 = pairSColl1Left.hashJoin(si)
   *   val joined2 = pairSColl2Left.hashJoin(si)
   * }}}
   *
   * @group join
   */
  def hashJoin[W](
    sideInput: SideInput[Map[K, Iterable[W]]]
  ): SCollection[(K, (V, W))] = {
    implicit val wCoder = BeamCoders.getMultiMapKV(sideInput)._2
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
  }

  /**
   * Perform a left outer join by replicating `rhs` to all workers. The right side should be tiny
   * and fit in memory.
   *
   * @example
   * {{{
   *   val si = pairSCollRight  // Should be tiny
   *   val joined = pairSColl1Left.hashLeftOuterJoin(pairSCollRight)
   * }}}
   * @group join
   * @param rhs The tiny SCollection[(K, W)] treated as right side of the join.
   */
  def hashLeftOuterJoin[W](
    rhs: SCollection[(K, W)]
  ): SCollection[(K, (V, Option[W]))] =
    hashLeftOuterJoin(rhs.asMultiMapSingletonSideInput)

  /**
   * Perform a left outer join with a MultiMap `SideInput[Map[K, Iterable[V]]`
   *
   * @example
   * {{{
   *   val si = pairSCollRight.asMultiMapSingletonSideInput
   *   val joined1 = pairSColl1Left.hashLeftOuterJoin(si)
   *   val joined2 = pairSColl2Left.hashLeftOuterJoin(si)
   * }}}
   * @group join
   */
  def hashLeftOuterJoin[W](
    sideInput: SideInput[Map[K, Iterable[W]]]
  ): SCollection[(K, (V, Option[W]))] = {
    implicit val wCoder = BeamCoders.getMultiMapKV(sideInput)._2
    self.transform { in =>
      in.withSideInputs(sideInput)
        .flatMap[(K, (V, Option[W]))] { case ((k, v), sideInputCtx) =>
          val rhsSideMap = sideInputCtx(sideInput)
          if (rhsSideMap.contains(k)) rhsSideMap(k).iterator.map(w => (k, (v, Some(w))))
          else Iterator((k, (v, None)))
        }
        .toSCollection
    }
  }

  /**
   * Perform a full outer join by replicating `rhs` to all workers. The right side should be tiny
   * and fit in memory.
   *
   * @group join
   */
  def hashFullOuterJoin[W](
    rhs: SCollection[(K, W)]
  ): SCollection[(K, (Option[V], Option[W]))] =
    hashFullOuterJoin(rhs.asMultiMapSingletonSideInput)

  /**
   * Perform a full outer join with a `SideInput[Map[K, Iterable[W]]]`.
   *
   * @example
   * {{{
   *   val si = pairSCollRight.asMultiMapSingletonSideInput
   *   val joined1 = pairSColl1Left.hashFullOuterJoin(si)
   *   val joined2 = pairSColl2Left.hashFullOuterJoin(si)
   * }}}
   *
   * @group join
   */
  def hashFullOuterJoin[W](
    sideInput: SideInput[Map[K, Iterable[W]]]
  ): SCollection[(K, (Option[V], Option[W]))] = {
    implicit val wCoder = BeamCoders.getMultiMapKV(sideInput)._2
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
  }

  /**
   * Return an SCollection with the pairs from `this` whose keys are in `rhs`
   * given `rhs` is small enough to fit in memory.
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @group per key
   */
  def hashIntersectByKey(
    rhs: SCollection[K]
  ): SCollection[(K, V)] =
    hashIntersectByKey(rhs.asSetSingletonSideInput)

  /**
   * Return an SCollection with the pairs from `this` whose keys are in the SideSet `rhs`.
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @group per key
   */
  def hashIntersectByKey(
    sideInput: SideInput[Set[K]]
  ): SCollection[(K, V)] =
    self
      .withSideInputs(sideInput)
      .filter { case ((k, _), sideInputCtx) => sideInputCtx(sideInput).contains(k) }
      .toSCollection

  /**
   * Return an SCollection with the pairs from `this` whose keys are not in SideInput[Set] `rhs`.
   *
   * @group per key
   */
  def hashSubtractByKey(
    sideInput: SideInput[Set[K]]
  ): SCollection[(K, V)] =
    self
      .withSideInputs(sideInput)
      .filter { case ((k, _), sideInputCtx) => !sideInputCtx(sideInput).contains(k) }
      .toSCollection

  /**
   * Return an SCollection with the pairs from `this` whose keys are not in SCollection[V] `rhs`.
   *
   * Rhs must be small enough to fit into memory.
   *
   * @group per key
   */
  def hashSubtractByKey(
    rhs: SCollection[K]
  ): SCollection[(K, V)] =
    hashSubtractByKey(rhs.asSetSingletonSideInput)

}
