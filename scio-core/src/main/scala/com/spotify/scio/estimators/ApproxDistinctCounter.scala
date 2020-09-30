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

package com.spotify.scio.estimators

import com.spotify.scio.coders.Coder
import com.spotify.scio.util.TupleFunctions._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.{transforms => beam}

/**
 * Approximate distinct element counter for type `T`, e.g. HyperLogLog or HyperLogLog++. This has two APIs one
 * estimate total distinct count for a given SCollection and second one estimate distinct count per each key in a
 * key-value SCollection.
 *
 * @tparam T
 */
trait ApproxDistinctCounter[T] {

  /**
   * Return a SCollection with single (Long)value which is the estimated distinct count in the given SCollection with
   * type `T`
   */
  def estimateDistinctCount(in: SCollection[T]): SCollection[Long]

  /**
   * Approximate distinct element per each key in the given key value SCollection.
   * This will output estimated distinct count per each unique key.
   */
  def estimateDistinctCountPerKey[K](
    in: SCollection[(K, T)]
  )(implicit koder: Coder[K], voder: Coder[T]): SCollection[(K, Long)]
}

/**
 * ApproxDistinctCounter impl for [[org.apache.beam.sdk.transforms.ApproximateUnique]] with sample size.
 *
 * Count approximate number of distinct values for each key in the SCollection.
 * @param sampleSize the number of entries in the statistical sample; the higher this number, the
 * more accurate the estimate will be; should be `>= 16`.
 */
case class ApproximateUniqueCounter[T](sampleSize: Int) extends ApproxDistinctCounter[T] {

  override def estimateDistinctCount(in: SCollection[T]): SCollection[Long] =
    in.applyTransform(beam.ApproximateUnique.globally(sampleSize))
      .asInstanceOf[SCollection[Long]]

  override def estimateDistinctCountPerKey[K](
    in: SCollection[(K, T)]
  )(implicit koder: Coder[K], voder: Coder[T]): SCollection[(K, Long)] =
    in.toKV
      .applyTransform(beam.ApproximateUnique.perKey[K, T](sampleSize))
      .map(klToTuple)
}

/**
 * ApproxDistinctCounter impl for [[org.apache.beam.sdk.transforms.ApproximateUnique]] with maximum estimation error.
 *
 * Count approximate number of distinct elements in the SCollection.
 * @param maximumEstimationError the maximum estimation error, which should be in the range
 * `[0.01, 0.5]`
 */
case class ApproximateUniqueCounterByError[T](maximumEstimationError: Double = 0.02)
    extends ApproxDistinctCounter[T] {

  override def estimateDistinctCount(in: SCollection[T]): SCollection[Long] =
    in.applyTransform(beam.ApproximateUnique.globally(maximumEstimationError))
      .asInstanceOf[SCollection[Long]]

  override def estimateDistinctCountPerKey[K](
    in: SCollection[(K, T)]
  )(implicit koder: Coder[K], voder: Coder[T]): SCollection[(K, Long)] =
    in.toKV
      .applyTransform(beam.ApproximateUnique.perKey[K, T](maximumEstimationError))
      .map(klToTuple)

}
