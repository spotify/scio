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

import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.util.FunctionsWithAccumulator

import scala.reflect.ClassTag

/**
 * An enhanced SCollection that provides access to one or more [[Accumulator]]s for some
 * transforms. [[Accumulator]]s are accessed via the additional [[AccumulatorContext]] argument.
 */
class SCollectionWithAccumulator[T: ClassTag] private[values]
(val internal: PCollection[T], val context: ScioContext, acc: Seq[Accumulator[_]])
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  /** [[SCollection.filter]] with an additional AccumulatorContext argument. */
  def filter(f: (T, AccumulatorContext) => Boolean): SCollectionWithAccumulator[T] = {
    val o = this.parDo(FunctionsWithAccumulator.filterFn(f, acc))
    new SCollectionWithAccumulator[T](o.internal, context, acc)
  }

  /** [[SCollection.flatMap]] with an additional AccumulatorContext argument. */
  def flatMap[U: ClassTag](f: (T, AccumulatorContext) => TraversableOnce[U])
  : SCollectionWithAccumulator[U] = {
    val o = this.parDo(FunctionsWithAccumulator.flatMapFn(f, acc))
    new SCollectionWithAccumulator[U](o.internal, context, acc)
  }

  /** [[SCollection.map]] with an additional AccumulatorContext argument. */
  def map[U: ClassTag](f: (T, AccumulatorContext) => U): SCollectionWithAccumulator[U] = {
    val o = this.parDo(FunctionsWithAccumulator.mapFn(f, acc))
    new SCollectionWithAccumulator[U](o.internal, context, acc)
  }

  /** Convert back to a basic SCollection. */
  def toSCollection: SCollection[T] = context.wrap(internal)

}
