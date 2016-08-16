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

import com.google.cloud.dataflow.sdk.transforms.Sum.SumLongFn
import com.spotify.scio.util.CallSites
import com.spotify.scio.values.{Accumulator, SCollection}

import scala.reflect.ClassTag

package object accumulators {

  /** Enhanced version of [[SCollection]] with accumulator helpers. */
  // TODO: scala 2.11
  // implicit class AccumulatorSCollection[T: ClassTag](private val self: SCollection[T]))
  // extends AnyVal {
  implicit class AccumulatorSCollection[T: ClassTag](val self: SCollection[T]) {

    /** Accumulate elements with the given accumulators. */
    def accumulate(as: Accumulator[T]*): SCollection[T] =
      self.withAccumulator(as: _*)
        .map { case (x, c) =>
          as.foreach(a => c.addValue(a, x))
          x
        }
        .toSCollection

    /** Accumulate elements with the given accumulators by applying `f`. */
    def accumulateBy[U](as: Accumulator[U]*)(f: T => U): SCollection[T] =
      self.withAccumulator(as: _*)
        .map { case (x, c) =>
          as.foreach(a => c.addValue(a, f(x)))
          x
        }
        .toSCollection

    /** Count elements with an accumulator. */
    def accumulateCount(acc: Accumulator[Long]): SCollection[T] = {
      require(acc.combineFn.isInstanceOf[SumLongFn], "acc must be a sum accumulator")
      self.accumulateBy(acc)(_ => 1L)
    }

    /** Count elements with an automatically created accumulator. */
    def accumulateCount: SCollection[T] =
      self.accumulateCount(self.context.sumAccumulator[Long](CallSites.getCurrentName))

    /** Count positive and negative results in a filter with the given accumulators. */
    def accumulateCountFilter(accPos: Accumulator[Long],
                              accNeg: Accumulator[Long])
                             (f: T => Boolean): SCollection[T] = {
      require(accPos.combineFn.isInstanceOf[SumLongFn], "accPos must be a sum accumulator")
      require(accNeg.combineFn.isInstanceOf[SumLongFn], "accNeg must be a sum accumulator")
      self.withAccumulator(accPos, accNeg)
        .filter { case (x, c) =>
          val b = f(x)
          c.addValue(if (b) accPos else accNeg, 1L)
          b
        }
        .toSCollection
    }

    /** Count positive and negative results in a filter with automatically created accumulators. */
    def accumulateCountFilter(f: T => Boolean): SCollection[T] = {
      val accPos = self.context.sumAccumulator[Long]("Positive#" + CallSites.getCurrentName)
      val accNeg = self.context.sumAccumulator[Long]("Negative#" + CallSites.getCurrentName)
      self.accumulateCountFilter(accPos, accNeg)(f)
    }

  }

}
