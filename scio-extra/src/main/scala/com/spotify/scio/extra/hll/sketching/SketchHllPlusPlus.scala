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

package com.spotify.scio.extra.hll.sketching

import com.spotify.scio.coders.{BeamCoders, Coder}
import com.spotify.scio.estimators.ApproxDistinctCounter
import com.spotify.scio.util.TupleFunctions.klToTuple
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sketching.ApproximateDistinct

/**
 * [[com.spotify.scio.estimators.ApproxDistinctCounter]] implementation for
 * [[org.apache.beam.sdk.extensions.sketching.ApproximateDistinct]], ApproximateDistinct estimate
 * the distinct count using HyperLogLog++.
 *
 * The HyperLogLog++ (HLL++) algorithm estimates the number of distinct values in a data stream.
 * HLL++ is based on HyperLogLog; HLL++ more accurately estimates the number of distinct values in
 * very large and small data streams.
 *
 * @param p
 *   Precision, Controls the accuracy of the estimation. The precision value will have an impact on
 *   the number of buckets used to store information about the distinct elements. In general one can
 *   expect a relative error of about 1.1 / sqrt(2&#94;p). The value should be of at least 4 to
 *   guarantee a minimal accuracy.
 * @param sp
 *   Sparse Precision, Uses to create a sparse representation in order to optimize memory and
 *   improve accuracy at small cardinalities. The value of sp should be greater than p(precision),
 *   but lower than 32.
 */
case class SketchHllPlusPlus[T](p: Int, sp: Int) extends ApproxDistinctCounter[T] {
  require(p > 4, "For better accuracy precision should be at least greater than 4")
  require(
    sp > p && sp < 32,
    "sparse precision(sp) should be greater than precision(p) and less than 32"
  )

  override def estimateDistinctCount(in: SCollection[T]): SCollection[Long] =
    in.applyTransform(
      "Approximate distinct count",
      ApproximateDistinct
        .globally[T]()
        .withPrecision(p)
        .withSparsePrecision(sp)
    ).asInstanceOf[SCollection[Long]]

  override def estimateDistinctCountPerKey[K](
    in: SCollection[(K, T)]
  ): SCollection[(K, Long)] = {
    implicit val keyCoder: Coder[K] = BeamCoders.getTupleCoders(in)._1

    in.toKV
      .applyTransform(
        "Approximate distinct count per key",
        ApproximateDistinct
          .perKey[K, T]()
          .withPrecision(p)
          .withSparsePrecision(sp)
      )
      .map(klToTuple)
  }
}
