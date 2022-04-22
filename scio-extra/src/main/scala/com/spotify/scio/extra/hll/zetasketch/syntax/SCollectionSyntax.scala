/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.extra.hll.zetasketch.syntax

import com.spotify.scio.extra.hll.zetasketch.ZetaSketchHll.ZetaSketchHllAggregator
import com.spotify.scio.extra.hll.zetasketch.{HllPlus, ZetaSketchHll}
import com.spotify.scio.values.SCollection

trait SCollectionSyntax {
  implicit final class ZetaSCollection[T](private val scol: SCollection[T]) {

    /**
     * Convert each element to [[ZetaSketchHll]]. Only support for Int, Long, String and ByteString
     * types.
     *
     * @Example
     *   {{{
     *   val input: SCollection[T] = ...
     *   val zCol: SCollection[ZetaSketchHll[T]] = input.asZetaSketchHll
     *   val approxDistCount: SCollection[Long] = zCol.sumHll.approxDistinctCount
     *   }}}
     *
     * [[ZetaSketchHll]] has few extra methods to access precision, sparse precision.
     *
     * @return
     *   [[SCollection]] of [[ZetaSketchHll]]. This will have the exactly the same number of element
     *   as input [[SCollection]]
     */
    def asZetaSketchHll(
      implicit
      hp: HllPlus[T]
    ): SCollection[ZetaSketchHll[T]] =
      scol.map(ZetaSketchHll.create[T](_))

    /**
     * Calculate the approximate distinct count using HyperLogLog++ algorithm. Only support for Int,
     * Long, String and ByteString types.
     *
     * @Example
     *   {{{
     *   val input: SCollection[T] = ...
     *   val approxDistCount: SCollection[Long] = input.approxDistinctCountWithZetaHll
     *   }}}
     *
     * @return
     *   - [[SCollection]] with one [[Long]] value.
     */
    def approxDistinctCountWithZetaHll(
      implicit
      hp: HllPlus[T]
    ): SCollection[Long] =
      scol.aggregate(ZetaSketchHllAggregator())
  }

  implicit final class PairedZetaSCollection[K, V](private val kvScol: SCollection[(K, V)]) {

    /**
     * Convert each value in key-value pair to [[ZetaSketchHll]]. Only support for Int, Long, String
     * and ByteString value types.
     *
     * @Example
     *   {{{
     *   val input: SCollection[(K, V)] = ...
     *   val zCol: SCollection[(K, ZetaSketchHll[V])] = input.asZetaSketchHllByKey
     *   val approxDistCount: SCollection[(K, Long)] = zCol.sumHllByKey.approxDistinctCountByKey
     *   }}}
     *
     * [[ZetaSketchHll]] has few extra methods to access precision, sparse precision.
     *
     * @return
     *   key-value [[SCollection]] where value being [[ZetaSketchHll]]. This will have the similar
     *   number of elements as input [[SCollection]].
     */
    def asZetaSketchHllByKey(
      implicit
      hp: HllPlus[V]
    ): SCollection[(K, ZetaSketchHll[V])] =
      kvScol.mapValues(ZetaSketchHll.create[V](_))

    /**
     * Calculate the approximate distinct count using HyperLogLog++ algorithm. Only support for Int,
     * Long, String and ByteString value types.
     *
     * @Example
     *   {{{
     *   val input: SCollection[(K, V)] = ...
     *   val approxDistCount: SCollection[(K, Long)] = input.approxDistinctCountWithZetaHllByKey
     *   }}}
     *
     * @return
     *   - [[SCollection]] with one [[Long]] value per each unique key.
     */
    def approxDistinctCountWithZetaHllByKey(
      implicit
      hp: HllPlus[V]
    ): SCollection[(K, Long)] =
      kvScol.aggregateByKey(ZetaSketchHllAggregator())
  }

  implicit final class ZetaSketchHllSCollection[T](
    private val scol: SCollection[ZetaSketchHll[T]]
  ) {
    def sumHll: SCollection[ZetaSketchHll[T]] = scol.reduce(_.merge(_))

    def approxDistinctCount: SCollection[Long] = scol.map(_.estimateSize())
  }

  implicit final class ZetaSketchHllSCollectionKV[K, V](
    private val kvSCol: SCollection[(K, ZetaSketchHll[V])]
  ) {
    def sumHllByKey: SCollection[(K, ZetaSketchHll[V])] = kvSCol.reduceByKey(_.merge(_))

    def approxDistinctCountByKey: SCollection[(K, Long)] = kvSCol.mapValues(_.estimateSize())
  }
}
