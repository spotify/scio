/*
 * Copyright 2018 Spotify AB.
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

import com.spotify.scio.util.Functions
import com.spotify.scio.util.TupleFunctions._
import com.twitter.algebird.{Aggregator, Monoid, Semigroup}
import org.apache.beam.sdk.transforms.Combine.PerKeyWithHotKeyFanout
import org.apache.beam.sdk.transforms.{Combine, SerializableFunction}

import scala.reflect.ClassTag

/**
 * An enhanced SCollection that uses an intermediate node to combine "hot" keys partially before
 * performing the full combine.
 */
class SCollectionWithHotKeyFanout[K: ClassTag, V: ClassTag] private[values]
(private val self: PairSCollectionFunctions[K, V],
 private val hotKeyFanout: Either[K => Int, Int])
  extends TransformNameable {

  private def withFanout[K, I, O](combine: Combine.PerKey[K, I, O])
  : PerKeyWithHotKeyFanout[K, I, O] = this.hotKeyFanout match {
    case Left(f) =>
      combine.withHotKeyFanout(
        Functions.serializableFn(f).asInstanceOf[SerializableFunction[K, java.lang.Integer]])
    case Right(f) =>
      combine.withHotKeyFanout(f)
  }

  override def withName(name: String): this.type = {
    self.self.withName(name)
    this
  }

  /**
   * [[PairSCollectionFunctions.aggregateByKey[U]* PairSCollectionFunctions.aggregateByKey]] with
   * hot key fanout.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                                combOp: (U, U) => U): SCollection[(K, U)] =
    self.applyPerKey(
      withFanout(Combine.perKey(Functions.aggregateFn(zeroValue)(seqOp, combOp))),
      kvToTuple[K, U])

  /**
   * [[PairSCollectionFunctions.aggregateByKey[A,U]* PairSCollectionFunctions.aggregateByKey]]
   * with hot key fanout.
   */
  def aggregateByKey[A: ClassTag, U: ClassTag](aggregator: Aggregator[V, A, U])
  : SCollection[(K, U)] =
    self.self.context.wrap(self.self.internal).transform { in =>
      val a = aggregator  // defeat closure
      in.mapValues(a.prepare).sumByKey(a.semigroup).mapValues(a.present)
    }

  /** [[PairSCollectionFunctions.combineByKey]] with hot key fanout. */
  def combineByKey[C: ClassTag](createCombiner: V => C)
                               (mergeValue: (C, V) => C)
                               (mergeCombiners: (C, C) => C): SCollection[(K, C)] =
    self.applyPerKey(
      withFanout(Combine.perKey(Functions.combineFn(createCombiner, mergeValue, mergeCombiners))),
      kvToTuple[K, C])

  /**
   * [[PairSCollectionFunctions.foldByKey(zeroValue:V)* PairSCollectionFunctions.foldByKey]] with
   * hot key fanout.
   */
  def foldByKey(zeroValue: V)(op: (V, V) => V): SCollection[(K, V)] =
    self.applyPerKey(
      withFanout(Combine.perKey(Functions.aggregateFn(zeroValue)(op, op))),
      kvToTuple[K, V])

  /**
   * [[PairSCollectionFunctions.foldByKey(implicit* PairSCollectionFunctions.foldByKey]] with
   * hot key fanout.
   */
  def foldByKey(implicit mon: Monoid[V]): SCollection[(K, V)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.reduceFn(mon))), kvToTuple[K, V])

  /** [[PairSCollectionFunctions.reduceByKey]] with hot key fanout. */
  def reduceByKey(op: (V, V) => V): SCollection[(K, V)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.reduceFn(op))), kvToTuple[K, V])

  /** [[PairSCollectionFunctions.sumByKey]] with hot key fanout. */
  def sumByKey(implicit sg: Semigroup[V]): SCollection[(K, V)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.reduceFn(sg))), kvToTuple[K, V])

}
