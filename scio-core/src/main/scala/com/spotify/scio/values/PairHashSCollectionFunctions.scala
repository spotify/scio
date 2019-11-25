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

import com.spotify.scio.coders.Coder

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

/**
 * Extra functions available on SCollections of (key, value) pairs for hash based joins
 * through an implicit conversion.
 *
 * @groupname join Join Operations
 */
class PairHashSCollectionFunctions[K, V](val self: SCollection[(K, V)]) {
  /**
   * Perform an inner join by replicating `that` to all workers. The right side should be tiny and
   * fit in memory.
   *
   * @group join
   */
  def hashJoin[W: Coder](
    that: SCollection[(K, W)]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (V, W))] =
    hashJoin(that.asMultiMapSideInput)

  /**
   * Perform an inner join with a [[SideMap]].
   *
   * `SideMap`s are deprecated in favor of `SideInput[Map[K, Iterable[W]]]`.
   * Example replacement:
   * {{{
   *   val si = pairSCollRight.asMultiMapSideInput
   *   val joined1 = pairSColl1Left.hashJoin(si)
   *   val joined2 = pairSColl2Left.hashJoin(si)
   * }}}
   *
   * @group join
   */
  @deprecated(
    "Use SCollection[(K, V)]#hashJoin(that) or SCollection[(K, V)]#hashJoin(that.asMultiMapSideInput) instead.",
    "0.8.0"
  )
  def hashJoin[W: Coder](
    sideMap: SideMap[K, W]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (V, W))] =
    hashJoin(sideMap.asImmutableSideInput)

  /**
   * Perform an inner join with a MultiMap `SideInput[Map[K, Iterable[V]]`
   *
   * The right side is tiny and fits in memory. The SideInput can be used reused for
   * multiple joins.
   *
   * Example:
   * {{{
   *   val si = pairSCollRight.asMultiMapSideInput
   *   val joined1 = pairSColl1Left.hashJoin(si)
   *   val joined2 = pairSColl2Left.hashJoin(si)
   * }}}
   *
   * @group join
   */
  def hashJoin[W: Coder](
    sideInput: SideInput[Map[K, Iterable[W]]]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (V, W))] =
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
   * Perform a left outer join by replicating `that` to all workers. The right side should be tiny
   * and fit in memory.
   *
   * @group join
   */
  def hashLeftJoin[W: Coder](
    that: SCollection[(K, W)]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (V, Option[W]))] =
    hashLeftJoin(that.asMultiMapSideInput)

  /**
   * Perform a left outer join with a [[SideMap]].
   *
   * SideMaps are deprecated in favor of `SideInput[Map[K, Iterable[W]]]`.
   * Example replacement:
   * {{{
   *   val si = pairSCollRight.asMultiMapSideInput
   *   val joined1 = pairSColl1Left.hashLeftJoin(si)
   *   val joined2 = pairSColl2Left.hashLeftJoin(si)
   * }}}
   *
   * @group join
   */
  @deprecated(
    "Use SCollection[(K, V)]#hashLeftJoin(that) or SCollection[(K, V)]#hashLeftJoin(that.asMultiMapSideInput) instead.",
    "0.8.0"
  )
  def hashLeftJoin[W: Coder](
    sideMap: SideMap[K, W]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (V, Option[W]))] =
    hashLeftJoin(sideMap.asImmutableSideInput)

  /**
   * Perform a left outer join with a MultiMap `SideInput[Map[K, Iterable[V]]`
   *
   * Example:
   * {{{
   *   val si = pairSCollRight.asMultiMapSideInput
   *   val joined1 = pairSColl1Left.hashLeftJoin(si)
   *   val joined2 = pairSColl2Left.hashLeftJoin(si)
   * }}}
   * @group join
   */
  def hashLeftJoin[W: Coder](
    sideInput: SideInput[Map[K, Iterable[W]]]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (V, Option[W]))] = self.transform {
    in =>
      in.withSideInputs(sideInput)
        .flatMap[(K, (V, Option[W]))] {
          case ((k, v), sideInputCtx) =>
            val thatSideMap = sideInputCtx(sideInput)
            if (thatSideMap.contains(k)) thatSideMap(k).iterator.map(w => (k, (v, Some(w))))
            else Iterator((k, (v, None)))
        }
        .toSCollection
  }

  /**
   * Perform a full outer join by replicating `that` to all workers. The right side should be tiny
   * and fit in memory.
   *
   * @group join
   */
  def hashFullOuterJoin[W: Coder](
    that: SCollection[(K, W)]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (Option[V], Option[W]))] =
    hashFullOuterJoin(that.asMultiMapSideInput)

  /**
   * Perform a full outer join with a [[SideMap]].
   *
   * SideMaps are deprecated in favour of `SideInput[ Map [ K, Iterable[W] ] ]`.
   * Example replacement:
   * {{{
   *   val si = pairSCollRight.asMultiMapSideInput
   *   val joined1 = pairSColl1Left.hashFullOuterJoin(si)
   *   val joined2 = pairSColl2Left.hashFullOuterJoin(si)
   * }}}
   *
   * @group join
   */
  @deprecated(
    "Use SCollection[(K, V)]#hashFullOuterJoin(that) or SCollection[(K, V)]#hashFullOuterJoin(that.asMultiMapSideInput) instead.",
    "0.8.0"
  )
  def hashFullOuterJoin[W: Coder](
    sideMap: SideMap[K, W]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (Option[V], Option[W]))] =
    hashFullOuterJoin(sideMap.asImmutableSideInput)

  /**
   * Perform a full outer join with a SideMap.
   *
   * Example:
   * {{{
   *   val si = pairSCollRight.asMultiMapSideInput
   *   val joined1 = pairSColl1Left.hashFullOuterJoin(si)
   *   val joined2 = pairSColl2Left.hashFullOuterJoin(si)
   * }}}
   *
   * @group join
   */
  def hashFullOuterJoin[W: Coder](
    sideInput: SideInput[Map[K, Iterable[W]]]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (Option[V], Option[W]))] =
    self.transform { in =>
      val leftHashed = in
        .withSideInputs(sideInput)
        .flatMap {
          case ((k, v), sideInputCtx) =>
            val thatSideMap = sideInputCtx(sideInput)
            if (thatSideMap.contains(k)) {
              thatSideMap(k).iterator
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
   * Return an SCollection with the pairs from `this` whose keys are in `that`
   * given `that` is small enough to fit in memory.
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @group per key
   */
  def hashIntersectByKey(
    that: SCollection[K]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, V)] =
    hashIntersectByKey(that.asSetSingletonSideInput)

  /**
   * Return an SCollection with the pairs from `this` whose keys are in the SideSet `that`.
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @group per key
   */
  @deprecated(
    "Use SCollection[(K, V)]#hashIntersectByKey(that.asSetSingletonSideInput) instead",
    "0.8.0"
  )
  def hashIntersectByKey(
    sideSet: SideSet[K]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, V)] =
    hashIntersectByKey(sideSet.side)

  /**
   * Return an SCollection with the pairs from `this` whose keys are in the SideSet `that`.
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @group per key
   */
  def hashIntersectByKey(
    sideInput: SideInput[Set[K]]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, V)] =
    self
      .withSideInputs(sideInput)
      .filter { case ((k, _), sideInputCtx) => sideInputCtx(sideInput).contains(k) }
      .toSCollection

  @deprecated("Use SCollection[(K, V)]#asMultiMapSideInput instead", "0.8.0")
  def toSideMap(implicit koder: Coder[K], voder: Coder[V]): SideMap[K, V] =
    SideMap[K, V](combineAsMapSideInput(self))

  private def combineAsMapSideInput[W: Coder](
    that: SCollection[(K, W)]
  )(implicit koder: Coder[K]): SideInput[MMap[K, ArrayBuffer[W]]] = {
    that
      .combine {
        case (k, v) =>
          MMap(k -> ArrayBuffer(v))
      } {
        case (combiner, (k, v)) =>
          combiner.getOrElseUpdate(k, ArrayBuffer.empty[W]) += v
          combiner
      } {
        case (left, right) =>
          right.foreach {
            case (k, vs) => left.getOrElseUpdate(k, ArrayBuffer.empty[W]) ++= vs
          }
          left
      }
      .asSingletonSideInput(MMap.empty[K, ArrayBuffer[W]])
  }
}
