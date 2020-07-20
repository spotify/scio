/*
 * Copyright 2019 Spotify AB.
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
import com.spotify.scio.coders.Coder
import com.spotify.scio.util.Functions
import com.spotify.scio.util.TupleFunctions._
import com.twitter.algebird.{Aggregator, Monoid, MonoidAggregator, Semigroup}
import org.apache.beam.sdk.transforms.Combine.PerKeyWithHotKeyFanout
import org.apache.beam.sdk.transforms.{Combine, SerializableFunction}

/**
 * An enhanced SCollection that uses an intermediate node to combine "hot" keys partially before
 * performing the full combine.
 */
class SCollectionWithHotKeyFanout[K, V] private[values] (
  private val context: ScioContext,
  private val self: PairSCollectionFunctions[K, V],
  private val hotKeyFanout: Either[K => Int, Int]
) extends TransformNameable {
  implicit private[this] val (keyCoder, valueCoder): (Coder[K], Coder[V]) =
    KvCoders.get(self.self)

  private def withFanout[K0, I, O](
    combine: Combine.PerKey[K0, I, O]
  ): PerKeyWithHotKeyFanout[K0, I, O] =
    this.hotKeyFanout match {
      case Left(f) =>
        combine.withHotKeyFanout(
          Functions
            .serializableFn(f)
            .asInstanceOf[SerializableFunction[K0, java.lang.Integer]]
        )
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
  def aggregateByKey[U: Coder](
    zeroValue: U
  )(seqOp: (U, V) => U, combOp: (U, U) => U): SCollection[(K, U)] =
    self.applyPerKey(
      withFanout(Combine.perKey(Functions.aggregateFn(context, zeroValue)(seqOp, combOp)))
    )(
      kvToTuple
    )

  /**
   * [[PairSCollectionFunctions.aggregateByKey[A,U]* PairSCollectionFunctions.aggregateByKey]]
   * with hot key fanout.
   */
  def aggregateByKey[A: Coder, U: Coder](aggregator: Aggregator[V, A, U]): SCollection[(K, U)] =
    self.self.context.wrap(self.self.internal).transform { in =>
      val a = aggregator // defeat closure
      in.mapValues(a.prepare)
        .sumByKey(a.semigroup)
        .mapValues(a.present)
    }

  /**
   * [[PairSCollectionFunctions.aggregateByKey[A,U]* PairSCollectionFunctions.aggregateByKey]]
   * with hot key fanout.
   */
  def aggregateByKey[A: Coder, U: Coder](
    aggregator: MonoidAggregator[V, A, U]
  ): SCollection[(K, U)] =
    self.self.context.wrap(self.self.internal).transform { in =>
      val a = aggregator // defeat closure
      in.mapValues(a.prepare)
        .foldByKey(a.monoid)
        .mapValues(a.present)
    }

  /** [[PairSCollectionFunctions.combineByKey]] with hot key fanout. */
  def combineByKey[C: Coder](
    createCombiner: V => C
  )(mergeValue: (C, V) => C)(mergeCombiners: (C, C) => C): SCollection[(K, C)] = {
    SCollection.logger.warn(
      "combineByKey/sumByKey does not support default value and may fail in some streaming " +
        "scenarios. Consider aggregateByKey/foldByKey instead."
    )
    self.applyPerKey(
      withFanout(
        Combine
          .perKey(Functions.combineFn(context, createCombiner, mergeValue, mergeCombiners))
      )
    )(kvToTuple)
  }

  /**
   * [[PairSCollectionFunctions.foldByKey(zeroValue:V)* PairSCollectionFunctions.foldByKey]] with
   * hot key fanout.
   */
  def foldByKey(zeroValue: V)(op: (V, V) => V): SCollection[(K, V)] =
    self.applyPerKey(
      withFanout(Combine.perKey(Functions.aggregateFn(context, zeroValue)(op, op)))
    )(kvToTuple)

  /**
   * [[PairSCollectionFunctions.foldByKey(implicit* PairSCollectionFunctions.foldByKey]] with
   * hot key fanout.
   */
  def foldByKey(implicit mon: Monoid[V]): SCollection[(K, V)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.reduceFn(context, mon))))(
      kvToTuple
    )

  /** [[PairSCollectionFunctions.reduceByKey]] with hot key fanout. */
  def reduceByKey(op: (V, V) => V): SCollection[(K, V)] =
    self.applyPerKey(withFanout(Combine.perKey(Functions.reduceFn(context, op))))(kvToTuple)

  /** [[PairSCollectionFunctions.sumByKey]] with hot key fanout. */
  def sumByKey(implicit sg: Semigroup[V]): SCollection[(K, V)] = {
    SCollection.logger.warn(
      "combineByKey/sumByKey does not support default value and may fail in some streaming " +
        "scenarios. Consider aggregateByKey/foldByKey instead."
    )
    self.applyPerKey(withFanout(Combine.perKey(Functions.reduceFn(context, sg))))(kvToTuple)
  }
}
