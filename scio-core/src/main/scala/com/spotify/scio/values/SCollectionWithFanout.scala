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

package com.spotify.scio.values

import com.spotify.scio.ScioContext
import com.spotify.scio.util.Functions
import com.spotify.scio.coders.Coder
import com.twitter.algebird.{Aggregator, Monoid, Semigroup}
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.values.PCollection

/**
 * An enhanced SCollection that uses an intermediate node to combine parts of the data to reduce
 * load on the final global combine step.
 */
class SCollectionWithFanout[T: Coder] private[values] (val internal: PCollection[T],
                                                          val context: ScioContext,
                                                          private val fanout: Int)
  extends PCollectionWrapper[T] {

  /** [[SCollection.aggregate[U]* SCollection.aggregate]] with fan out. */
  def aggregate[U: Coder](zeroValue: U)(seqOp: (U, T) => U,
                                           combOp: (U, U) => U): SCollection[U] =
    this.pApply(
      Combine.globally(Functions.aggregateFn(zeroValue)(seqOp, combOp)).withFanout(fanout))

  /** [[SCollection.aggregate[A,U]* SCollection.aggregate]] with fan out. */
  def aggregate[A: Coder, U: Coder](aggregator: Aggregator[T, A, U]): SCollection[U] = {
    val a = aggregator  // defeat closure
    context.wrap(internal).transform(_.map(a.prepare).sum(a.semigroup, Coder[A]).map(a.present))
  }

  /** [[SCollection.combine]] with fan out. */
  def combine[C: Coder](createCombiner: T => C)
                          (mergeValue: (C, T) => C)
                          (mergeCombiners: (C, C) => C): SCollection[C] =
    this.pApply(
      Combine
        .globally(Functions.combineFn(createCombiner, mergeValue, mergeCombiners))
        .withFanout(fanout))

  /** [[SCollection.fold(zeroValue:T)* SCollection.fold]] with fan out. */
  def fold(zeroValue: T)(op: (T, T) => T): SCollection[T] =
    this.pApply(Combine.globally(Functions.aggregateFn(zeroValue)(op, op)).withFanout(fanout))

  /** [[SCollection.fold(implicit* SCollection.fold]] with fan out. */
  def fold(implicit mon: Monoid[T]): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(mon)).withFanout(fanout))

  /** [[SCollection.reduce]] with fan out. */
  def reduce(op: (T, T) => T): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(op)).withFanout(fanout))

  /** [[SCollection.sum]] with fan out. */
  def sum(implicit sg: Semigroup[T]): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(sg)).withFanout(fanout))

}
