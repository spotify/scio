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

package com.spotify.scio.extra.hll

import com.spotify.scio.coders.{BeamCoders, Coder}
import com.spotify.scio.estimators.ApproxDistinctCounter
import com.spotify.scio.util.TupleFunctions._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.zetasketch.HllCount

package object zetasketch {

  /**
   * [[com.spotify.scio.estimators.ApproxDistinctCounter]] implementation for
   * [[org.apache.beam.sdk.extensions.zetasketch.HllCount]],
   * specifically [[org.apache.beam.sdk.extensions.zetasketch.HllCount.Init#forIntegers]].
   * HllCount estimate the distinct count using HyperLogLogPlusPlus (HLL++) sketches on data streams based on
   * the ZetaSketch implementation.
   *
   * The HyperLogLog++ (HLL++) algorithm estimates the number of distinct values in a data stream.
   * HLL++ is based on HyperLogLog; HLL++ more accurately estimates the number of distinct values in very large and
   * small data streams.
   *
   * @param p Precision, controls the accuracy of the estimation. The precision value will have an impact on the number of buckets
   *          used to store information about the distinct elements.
   *          should be in the range `[10, 24]`, default precision value is `15`.
   */
  case class ZetasketchHllIntCounter(p: Int = HllCount.DEFAULT_PRECISION)
      extends ApproxDistinctCounter[Int] {

    override def estimateDistinctCount(in: SCollection[Int]): SCollection[Long] = {
      in.asInstanceOf[SCollection[Integer]]
        .applyTransform(HllCount.Init.forIntegers().withPrecision(p).globally())
        .applyTransform(HllCount.Extract.globally())
        .map(Long2long)
    }

    override def estimateDistinctCountPerKey[K](
      in: SCollection[(K, Int)]
    ): SCollection[(K, Long)] = {
      implicit val (keyCoder, _): (Coder[K], Coder[Int]) = BeamCoders.getTupleCoders(in)
      in.mapValues(int2Integer)
        .toKV
        .applyTransform(HllCount.Init.forIntegers().withPrecision(p).perKey())
        .applyTransform(HllCount.Extract.perKey())
        .map(klToTuple)
    }
  }

  /**
   * [[com.spotify.scio.estimators.ApproxDistinctCounter]] implementation for
   * [[org.apache.beam.sdk.extensions.zetasketch.HllCount]],
   * specifically [[org.apache.beam.sdk.extensions.zetasketch.HllCount.Init#forLongs]].
   * HllCount estimate the distinct count using HyperLogLogPlusPlus (HLL++) sketches on data streams based on
   * the ZetaSketch implementation.
   *
   * The HyperLogLog++ (HLL++) algorithm estimates the number of distinct values in a data stream.
   * HLL++ is based on HyperLogLog; HLL++ more accurately estimates the number of distinct values in very large and
   * small data streams.
   *
   * @param p Precision, controls the accuracy of the estimation. The precision value will have an impact on the number of buckets
   *          used to store information about the distinct elements.
   *          should be in the range `[10, 24]`, default precision value is `15`.
   */
  case class ZetasketchHllLongCounter(p: Int = HllCount.DEFAULT_PRECISION)
      extends ApproxDistinctCounter[Long] {

    override def estimateDistinctCount(in: SCollection[Long]): SCollection[Long] =
      in.asInstanceOf[SCollection[java.lang.Long]]
        .applyTransform(HllCount.Init.forLongs().withPrecision(p).globally())
        .applyTransform(HllCount.Extract.globally())
        .asInstanceOf[SCollection[Long]]

    override def estimateDistinctCountPerKey[K](
      in: SCollection[(K, Long)]
    ): SCollection[(K, Long)] = {
      implicit val (keyCoder, _): (Coder[K], Coder[Long]) = BeamCoders.getTupleCoders(in)
      in.mapValues(long2Long)
        .toKV
        .applyTransform(HllCount.Init.forLongs().withPrecision(p).perKey())
        .applyTransform(HllCount.Extract.perKey())
        .map(klToTuple)
    }
  }

  /**
   * [[com.spotify.scio.estimators.ApproxDistinctCounter]] implementation for
   * [[org.apache.beam.sdk.extensions.zetasketch.HllCount]],
   * specifically [[org.apache.beam.sdk.extensions.zetasketch.HllCount.Init#forStrings]].
   * HllCount estimate the distinct count using HyperLogLogPlusPlus (HLL++) sketches on data streams based on
   * the ZetaSketch implementation.
   *
   * The HyperLogLog++ (HLL++) algorithm estimates the number of distinct values in a data stream.
   * HLL++ is based on HyperLogLog; HLL++ more accurately estimates the number of distinct values in very large and
   * small data streams.
   *
   * @param p Precision, controls the accuracy of the estimation. The precision value will have an impact on the number of buckets
   *          used to store information about the distinct elements.
   *          should be in the range `[10, 24]`, default precision value is `15`.
   */
  case class ZetasketchHllStringCounter(p: Int = HllCount.DEFAULT_PRECISION)
      extends ApproxDistinctCounter[String] {

    override def estimateDistinctCount(in: SCollection[String]): SCollection[Long] =
      in.applyTransform(HllCount.Init.forStrings().withPrecision(p).globally())
        .applyTransform(HllCount.Extract.globally())
        .asInstanceOf[SCollection[Long]]

    override def estimateDistinctCountPerKey[K](
      in: SCollection[(K, String)]
    ): SCollection[(K, Long)] = {
      implicit val (keyCoder, _): (Coder[K], Coder[String]) = BeamCoders.getTupleCoders(in)
      in.toKV
        .applyTransform(HllCount.Init.forStrings().withPrecision(p).perKey())
        .applyTransform(HllCount.Extract.perKey())
        .map(klToTuple)
    }
  }

  /**
   * [[com.spotify.scio.estimators.ApproxDistinctCounter]] implementation for
   * [[org.apache.beam.sdk.extensions.zetasketch.HllCount]],
   * specifically [[org.apache.beam.sdk.extensions.zetasketch.HllCount.Init#forBytes]].
   * HllCount estimate the distinct count using HyperLogLogPlusPlus (HLL++) sketches on data streams based on
   * the ZetaSketch implementation.
   *
   * The HyperLogLog++ (HLL++) algorithm estimates the number of distinct values in a data stream.
   * HLL++ is based on HyperLogLog; HLL++ more accurately estimates the number of distinct values in very large and
   * small data streams.
   *
   * @param p Precision, controls the accuracy of the estimation. The precision value will have an impact on the number of buckets
   *          used to store information about the distinct elements.
   *          should be in the range `[10, 24]`, default precision value is `15`.
   */
  case class ZetasketchHllByteArrayCounter(p: Int = HllCount.DEFAULT_PRECISION)
      extends ApproxDistinctCounter[Array[Byte]] {

    override def estimateDistinctCount(in: SCollection[Array[Byte]]): SCollection[Long] =
      in.applyTransform(HllCount.Init.forBytes().withPrecision(p).globally())
        .applyTransform(HllCount.Extract.globally())
        .asInstanceOf[SCollection[Long]]

    override def estimateDistinctCountPerKey[K](
      in: SCollection[(K, Array[Byte])]
    ): SCollection[(K, Long)] = {
      implicit val (keyCoder, _): (Coder[K], Coder[Array[Byte]]) =
        BeamCoders.getTupleCoders(in)
      in.toKV
        .applyTransform(HllCount.Init.forBytes().withPrecision(p).perKey())
        .applyTransform(HllCount.Extract.perKey())
        .map(klToTuple)
    }

  }

}
