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
    hashJoin(SideMap(combineAsMapSideInput(that)))

  /**
   * Perform an inner join with a SideMap.
   *
   * @group join
   */
  def hashJoin[W: Coder](
    that: SideMap[K, W]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (V, W))] =
    self.transform { in =>
      val side = that.side
      implicit val vwCoder = Coder[(V, W)]
      in.withSideInputs(side)
        .flatMap[(K, (V, W))] { (kv, s) =>
          s(side)
            .getOrElse(kv._1, ArrayBuffer.empty[W])
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
    hashLeftJoin(SideMap(combineAsMapSideInput(that)))

  /**
   * Perform a left outer join with a SideMap.
   *
   * @group join
   */
  def hashLeftJoin[W: Coder](
    that: SideMap[K, W]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (V, Option[W]))] = self.transform {
    in =>
      val side = that.side
      implicit val vwCoder = Coder[(V, Option[W])]
      in.withSideInputs(side)
        .flatMap[(K, (V, Option[W]))] { (kv, s) =>
          val (k, v) = kv
          val m = s(side)
          if (m.contains(k)) m(k).iterator.map(w => (k, (v, Some(w))))
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
    hashFullOuterJoin(SideMap(combineAsMapSideInput(that)))

  /**
   * Perform a full outer join with a SideMap.
   *
   * @group join
   */
  def hashFullOuterJoin[W: Coder](
    that: SideMap[K, W]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, (Option[V], Option[W]))] =
    self.transform { in =>
      val side = that.side

      val leftHashed = in
        .withSideInputs(side)
        .flatMap { (kv, s) =>
          val (k, v) = kv
          val m = s(side)
          if (m.contains(k)) {
            m(k).iterator
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
        .withSideInputs(side)
        .flatMap { (mk, s) =>
          val m = s(side)
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
    hashIntersectByKey(that.toSideSet)

  /**
   * Return an SCollection with the pairs from `this` whose keys are in the SideSet `that`.
   *
   * Unlike [[SCollection.intersection]] this preserves duplicates in `this`.
   *
   * @group per key
   */
  def hashIntersectByKey(
    that: SideSet[K]
  )(implicit koder: Coder[K], voder: Coder[V]): SCollection[(K, V)] = {
    val side = that.side
    self
      .withSideInputs(side)
      .filter { case ((k, v), s) => s(side).contains(k) }
      .toSCollection
  }

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
