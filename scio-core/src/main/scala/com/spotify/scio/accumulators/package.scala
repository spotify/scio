/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio

import com.spotify.scio.util.CallSites
import com.spotify.scio.values.{Accumulator, SCollection}

import scala.reflect.ClassTag

/**
 * Main package for [[com.spotify.scio.values.Accumulator Accumulator]] helper APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.accumulators._
 * }}}
 */
package object accumulators {

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with
   * [[com.spotify.scio.values.Accumulator Accumulator]]
   * helpers.
   */
  implicit class AccumulatorSCollection[T: ClassTag](val self: SCollection[T]) {

    /**
     * Accumulate elements with the given [[com.spotify.scio.values.Accumulator Accumulator]]s.
     *
     * For example:
     *
     * {{{
     * val max = sc.maxAccumulator[Int]("max")
     * val min = sc.maxAccumulator[Int]("min")
     * sc.parallelize(1 to 100).accumulate(max, min)
     * }}}
     */
    def accumulate(as: Accumulator[T]*): SCollection[T] =
      self.withAccumulator(as: _*)
        .map { case (x, c) =>
          val i = as.iterator
          while (i.hasNext) c.addValue(i.next(), x)
          x
        }
        .toSCollection

    /**
     * Accumulate elements with the given [[com.spotify.scio.values.Accumulator Accumulator]]s by
     * applying `f`.
     *
     * For example:
     *
     * {{{
     * val max = sc.maxAccumulator[Int]("max")
     * val min = sc.maxAccumulator[Int]("min")
     * sc.textFile("input.txt").accumulateBy(max, min)(_.length)
     * }}}
     */
    def accumulateBy[U](as: Accumulator[U]*)(f: T => U): SCollection[T] =
      self.withAccumulator(as: _*)
        .map { case (x, c) =>
          val fx = f(x)
          val i = as.iterator
          while (i.hasNext) c.addValue(i.next(), fx)
          x
        }
        .toSCollection

    /** Count elements with an [[com.spotify.scio.values.Accumulator Accumulator]]. */
    def accumulateCount(acc: Accumulator[Long]): SCollection[T] = {
      require(acc.combineFn.getClass.getSimpleName == "SumLongFn", "acc must be a sum accumulator")
      self.accumulateBy(acc)(_ => 1L)
    }

    /**
     * Count elements with an automatically created
     * [[com.spotify.scio.values.Accumulator Accumulator]].
     */
    def accumulateCount: SCollection[T] =
      self.accumulateCount(self.context.sumAccumulator[Long](CallSites.getCurrentName))

    /**
     * Count positive and negative results in a
     * [[com.spotify.scio.values.SCollection.filter SCollection.filter]] with the given
     * [[com.spotify.scio.values.Accumulator Accumulator]]s.
     *
     * For example:
     *
     * {{{
     * val pos = sc.sumAccumulator[Long]("positive")
     * val neg = sc.sumAccumulator[Long]("negative")
     * sc.parallelize(1 to 100).accumulateCountFilter(pos, neg)(_ > 95)
     * }}}
     */
    def accumulateCountFilter(accPos: Accumulator[Long],
                              accNeg: Accumulator[Long])
                             (f: T => Boolean): SCollection[T] = {
      require(
        accPos.combineFn.getClass.getSimpleName == "SumLongFn",
        "accPos must be a sum accumulator")
      require(
        accNeg.combineFn.getClass.getSimpleName == "SumLongFn",
        "accNeg must be a sum accumulator")
      self.withAccumulator(accPos, accNeg)
        .filter { case (x, c) =>
          val b = f(x)
          c.addValue(if (b) accPos else accNeg, 1L)
          b
        }
        .toSCollection
    }

    /**
     * Count positive and negative results in a
     * [[com.spotify.scio.values.SCollection.filter SCollection.filter]] with automatically
     * created [[com.spotify.scio.values.Accumulator Accumulator]]s.
     */
    def accumulateCountFilter(f: T => Boolean): SCollection[T] = {
      val accPos = self.context.sumAccumulator[Long]("Positive#" + CallSites.getCurrentName)
      val accNeg = self.context.sumAccumulator[Long]("Negative#" + CallSites.getCurrentName)
      self.accumulateCountFilter(accPos, accNeg)(f)
    }

  }

}
