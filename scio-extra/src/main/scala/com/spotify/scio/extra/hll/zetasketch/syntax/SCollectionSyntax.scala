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

import com.spotify.scio.extra.hll.zetasketch.ZetaSketchHLL.ZetaSketchHLLAggregator
import com.spotify.scio.extra.hll.zetasketch.{HllPlus, ZetaSketchHLL}
import com.spotify.scio.values.SCollection

trait SCollectionSyntax {
  implicit final class ZetaSCollection[T](private val scol: SCollection[T]) {

    /**
     * Convert each element to [[ZetaSketchHLL]].
     * Only support for Int, Long, String and ByteString types.
     *
     * @Example
     * {{{
     *   val input: SCollection[T] = ...
     *   val zCol: SCollection[ZetaSketchHll[T]] = input.asZetaSketchHll
     *   val approxDistCount: SCollection[Long] = zCol.sumHll.approxDistinctCount
     * }}}
     *
     * [[ZetaSketchHLL]] has few extra methods to access precision, sparse precision.
     *
     * @return [[SCollection]] of [[ZetaSketchHLL]].
     *         This will have the exactly the same number of element as input [[SCollection]]
     */
    def asZetaSketchHLL(implicit hp: HllPlus[T]): SCollection[ZetaSketchHLL[T]] =
      scol.map(ZetaSketchHLL.create[T](_))

    /**
     * Calculate the approximate distinct count using HyperLogLog++ algorithm.
     * Only support for Int, Long, String and ByteString types.
     *
     * @Example
     * {{{
     *   val input: SCollection[T] = ...
     *   val approxDistCount: SCollection[Long] = input.approxDistinctCountWithZetaHll
     * }}}
     *
     * @return - [[SCollection]] with one [[Long]] value.
     */
    def approxDistinctCountWithZetaHll(implicit hp: HllPlus[T]): SCollection[Long] =
      scol.aggregate(ZetaSketchHLLAggregator())
  }

  implicit final class PairedZetaSCollection[K, V](private val kvScol: SCollection[(K, V)]) {

    /**
     * Convert each value in key-value pair to [[ZetaSketchHLL]].
     * Only support for Int, Long, String and ByteString value types.
     *
     * @Example
     * {{{
     *   val input: SCollection[(K, V)] = ...
     *   val zCol: SCollection[(K, ZetaSketchHLL[V])] = input.asZetaSketchHllByKey
     *   val approxDistCount: SCollection[(K, Long)] = zCol.sumHllByKey.approxDistinctCountByKey
     * }}}
     *
     * [[ZetaSketchHLL]] has few extra methods to access precision, sparse precision.
     *
     * @return key-value [[SCollection]] where value being [[ZetaSketchHLL]].
     *         This will have the similar number of elements as input [[SCollection]].
     */
    def asZetaSketchHLLByKey(implicit hp: HllPlus[V]): SCollection[(K, ZetaSketchHLL[V])] =
      kvScol.mapValues(ZetaSketchHLL.create[V](_))

    /**
     * Calculate the approximate distinct count using HyperLogLog++ algorithm.
     * Only support for Int, Long, String and ByteString value types.
     *
     * @Example
     * {{{
     *   val input: SCollection[(K, V)] = ...
     *   val approxDistCount: SCollection[(K, Long)] = input.approxDistinctCountWithZetaHllByKey
     * }}}
     *
     * @return - [[SCollection]] with one [[Long]] value per each unique key.
     */
    def approxDistinctCountWithZetaHllByKey(implicit hp: HllPlus[V]): SCollection[(K, Long)] =
      kvScol.aggregateByKey(ZetaSketchHLLAggregator())
  }

  implicit final class ZetaSketchHLLSCollection[T](
    private val scol: SCollection[ZetaSketchHLL[T]]
  ) {
    def sumHll: SCollection[ZetaSketchHLL[T]] = scol.reduce(_.merge(_))

    def approxDistinctCount: SCollection[Long] = scol.map(_.estimateSize())
  }

  implicit final class ZetaSketchHLLSCollectionKV[K, V](
    private val kvSCol: SCollection[(K, ZetaSketchHLL[V])]
  ) {
    def sumHllByKey: SCollection[(K, ZetaSketchHLL[V])] = kvSCol.reduceByKey(_.merge(_))

    def approxDistinctCountByKey: SCollection[(K, Long)] = kvSCol.mapValues(_.estimateSize())
  }
}
