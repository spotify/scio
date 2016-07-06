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

import com.google.cloud.dataflow.sdk.transforms.Combine
import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.util.Functions
import com.twitter.algebird.{Semigroup, Monoid}

import scala.reflect.ClassTag

/**
 * An enhanced SCollection that uses an intermediate node to combine parts of the data to reduce
 * load on the final global combine step.
 */
class SCollectionWithFanout[T: ClassTag] private[values] (val internal: PCollection[T],
                                                          val context: ScioContext,
                                                          private val fanout: Int)
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  /**
   * Aggregate the elements using given combine functions and a neutral "zero value". This
   * function can return a different result type, U, than the type of this SCollection, T. Thus,
   * we need one operation for merging a T into an U and one operation for merging two U's. Both
   * of these functions are allowed to modify and return their first argument instead of creating
   * a new U to avoid memory allocation.
   */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U,
                                           combOp: (U, U) => U): SCollection[U] =
    this.pApply(
      Combine.globally(Functions.aggregateFn(zeroValue)(seqOp, combOp)).withFanout(fanout))

  /**
   * Generic function to combine the elements using a custom set of aggregation functions. Turns
   * an SCollection[T] into a result of type SCollection[C], for a "combined type" C. Note that V
   * and C can be different -- for example, one might combine an SCollection of type Int into an
   * SCollection of type Seq[Int]. Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *
   * - `mergeCombiners`, to combine two C's into a single one.
   */
  def combine[C: ClassTag](createCombiner: T => C)
                          (mergeValue: (C, T) => C)
                          (mergeCombiners: (C, C) => C): SCollection[C] =
    this.pApply(
      Combine
        .globally(Functions.combineFn(createCombiner, mergeValue, mergeCombiners))
        .withFanout(fanout))

  /**
   * Aggregate the elements using a given associative function and a neutral "zero value". The
   * function op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   */
  def fold(zeroValue: T)(op: (T, T) => T): SCollection[T] =
    this.pApply(Combine.globally(Functions.aggregateFn(zeroValue)(op, op)).withFanout(fanout))

  /**
   * Fold with [[com.twitter.algebird.Monoid Monoid]], which defines the associative function and
   * "zero value" for T. This could be more powerful and better optimized in some cases.
   */
  def fold(implicit mon: Monoid[T]): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(mon)).withFanout(fanout))

  /**
   * Reduce the elements of this SCollection using the specified commutative and associative
   * binary operator.
   */
  def reduce(op: (T, T) => T): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(op)).withFanout(fanout))

  /**
   * Reduce with [[com.twitter.algebird.Semigroup Semigroup]]. This could be more powerful and
   * better optimized in some cases.
   */
  def sum(implicit sg: Semigroup[T]): SCollection[T] =
    this.pApply(Combine.globally(Functions.reduceFn(sg)).withFanout(fanout))

}
