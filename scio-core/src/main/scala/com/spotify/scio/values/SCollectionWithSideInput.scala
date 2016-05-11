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

import com.google.cloud.dataflow.sdk.transforms.ParDo
import com.google.cloud.dataflow.sdk.values.{TupleTag, _}
import com.spotify.scio.ScioContext
import com.spotify.scio.util.{CallSites, FunctionsWithSideInput}
import com.spotify.scio.values.SPartition.SPartition

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try

/**
 * An enhanced SCollection that provides access to one or more [[SideInput]]s for some transforms.
 * [[SideInput]]s are accessed via the additional [[SideInputContext]] argument.
 */
class SCollectionWithSideInput[T: ClassTag] private[values] (val internal: PCollection[T],
                                                             private[scio] val context: ScioContext,
                                                             sides: Iterable[SideInput[_]])
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  private val parDo = ParDo.withSideInputs(sides.map(_.view).asJava)

  /** [[SCollection.filter]] with an additional SideInputContext argument. */
  def filter(f: (T, SideInputContext[T]) => Boolean): SCollectionWithSideInput[T] = {
    val o = this
      .apply(parDo.of(FunctionsWithSideInput.filterFn(f)))
      .internal.setCoder(this.getCoder[T])
    new SCollectionWithSideInput[T](o, context, sides)
  }

  /** [[SCollection.flatMap]] with an additional SideInputContext argument. */
  def flatMap[U: ClassTag](f: (T, SideInputContext[T]) => TraversableOnce[U])
  : SCollectionWithSideInput[U] = {
    val o = this
      .apply(parDo.of(FunctionsWithSideInput.flatMapFn(f)))
      .internal.setCoder(this.getCoder[U])
    new SCollectionWithSideInput[U](o, context, sides)
  }

  /** [[SCollection.keyBy]] with an additional SideInputContext argument. */
  def keyBy[K: ClassTag](f: (T, SideInputContext[T]) => K): SCollectionWithSideInput[(K, T)] =
    this.map((x, s) => (f(x, s), x))

  /** [[SCollection.map]] with an additional SideInputContext argument. */
  def map[U: ClassTag](f: (T, SideInputContext[T]) => U): SCollectionWithSideInput[U] = {
    val o = this
      .apply(parDo.of(FunctionsWithSideInput.mapFn(f)))
      .internal.setCoder(this.getCoder[U])
    new SCollectionWithSideInput[U](o, context, sides)
  }

  /**
   * [[SCollection.partition]] with an additional SideInputContext argument.
 *
   *  @return map of partition to side output [[SCollection]]
   * */
  def partition(partitions: Seq[SPartition[T]], f: (T, Seq[SPartition[T]], SideInputContext[T])
    => SPartition[T]): Map[SPartition[T], SCollection[T]] = {

    val tagToSide = partitions.map(e => e.tupleTag.getId -> e).toMap
    val sideTags = TupleTagList.of(partitions.map(e => e.tupleTag.asInstanceOf[TupleTag[_]]).asJava)

    val transform = parDo
      .withOutputTags(new TupleTag[T](), sideTags)
      .of(FunctionsWithSideInput.partitionFn[T](partitions, f))

    // Main tag is ignored
    val pCollectionWrapper = this.internal.apply(CallSites.getCurrent, transform)
    pCollectionWrapper.getAll.asScala
      .mapValues(context.wrap(_).asInstanceOf[SCollection[T]].setCoder(internal.getCoder))
      .flatMap{ case(tt, col) => Try{ tagToSide(tt.getId) -> col}.toOption}
      .toMap
  }

  /** Convert back to a basic SCollection. */
  def toSCollection: SCollection[T] = context.wrap(internal)

}
