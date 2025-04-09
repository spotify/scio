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
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.transforms.BatchDoFn
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.scio.util.FunctionsWithSideInput.{
  CollectFnWithResource,
  FilterFnWithResource,
  FlatMapFnWithResource,
  MapFnWithWithResource,
  SideInputDoFn
}
import com.spotify.scio.util.{FunctionsWithSideInput, ScioUtil}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{PCollection, TupleTag, TupleTagList}

import scala.jdk.CollectionConverters._
import scala.collection.compat._
import scala.util.Try
import com.twitter.chill.ClosureCleaner

/**
 * An enhanced SCollection that provides access to one or more [[SideInput]] s for some transforms.
 * [[SideInput]] s are accessed via the additional [[SideInputContext]] argument.
 */
class SCollectionWithSideInput[T] private[values] (
  coll: SCollection[T],
  sides: Iterable[SideInput[_]]
) extends PCollectionWrapper[T] {
  override val internal: PCollection[T] = coll.internal

  override val context: ScioContext = coll.context

  override def withName(name: String): this.type = {
    coll.withName(name)
    this
  }

  private def parDo[T0, U](fn: DoFn[T0, U]) =
    ParDo.of(fn).withSideInputs(sides.map(_.view).asJava)

  /** [[SCollection.filter]] with an additional [[SideInputContext]] argument. */
  def filter(f: (T, SideInputContext[T]) => Boolean): SCollectionWithSideInput[T] = {
    val o = coll
      .pApply(parDo(FunctionsWithSideInput.filterFn(f)))
      .setCoder(internal.getCoder)
    new SCollectionWithSideInput(o, sides)
  }

  /** [[SCollection.flatMap]] with an additional [[SideInputContext]] argument. */
  def flatMap[U: Coder](
    f: (T, SideInputContext[T]) => TraversableOnce[U]
  ): SCollectionWithSideInput[U] = {
    val o = coll
      .pApply(parDo(FunctionsWithSideInput.flatMapFn(f)))
      .setCoder(CoderMaterializer.beam(context, Coder[U]))
    new SCollectionWithSideInput[U](o, sides)
  }

  /** [[SCollection.keyBy]] with an additional [[SideInputContext]] argument. */
  def keyBy[K: Coder](f: (T, SideInputContext[T]) => K): SCollectionWithSideInput[(K, T)] =
    this.map((x, s) => (f(x, s), x))

  /** [[SCollection.map]] with an additional [[SideInputContext]] argument. */
  def map[U: Coder](f: (T, SideInputContext[T]) => U): SCollectionWithSideInput[U] = {
    val o = coll
      .pApply(parDo(FunctionsWithSideInput.mapFn(f)))
      .setCoder(CoderMaterializer.beam(context, Coder[U]))
    new SCollectionWithSideInput[U](o, sides)
  }

  /** [[SCollection.collect]] with an additional [[SideInputContext]] argument. */
  def collect[U: Coder](
    f: PartialFunction[(T, SideInputContext[T]), U]
  ): SCollectionWithSideInput[U] = {
    val o = coll
      .pApply(parDo(FunctionsWithSideInput.partialFn(f)))
      .setCoder(CoderMaterializer.beam(context, Coder[U]))
    new SCollectionWithSideInput[U](o, sides)
  }

  /** [[filter]] with additional resource arguments. */
  def filterWithResource[R](resource: => R, resourceType: ResourceType)(
    f: (R, T, SideInputContext[T]) => Boolean
  ): SCollectionWithSideInput[T] = {
    val o = coll
      .pApply(parDo(new FilterFnWithResource(resource, resourceType, f)))
      .setCoder(internal.getCoder)
    new SCollectionWithSideInput(o, sides)
  }

  /** [[flatMap]] with additional resource arguments. */
  def flatMapWithResource[R, U: Coder](resource: => R, resourceType: ResourceType)(
    f: (R, T, SideInputContext[T]) => TraversableOnce[U]
  ): SCollectionWithSideInput[U] = {
    val o = coll
      .pApply(parDo(new FlatMapFnWithResource(resource, resourceType, f)))
      .setCoder(CoderMaterializer.beam(context, Coder[U]))
    new SCollectionWithSideInput[U](o, sides)
  }

  /** [[map]] with additional resource arguments. */
  def mapWithResource[R, U: Coder](resource: => R, resourceType: ResourceType)(
    fn: (R, T, SideInputContext[T]) => U
  ): SCollectionWithSideInput[U] = {
    val o = coll
      .pApply(parDo(new MapFnWithWithResource(resource, resourceType, fn)))
      .setCoder(CoderMaterializer.beam(context, Coder[U]))
    new SCollectionWithSideInput[U](o, sides)
  }

  /** [[collect]] with additional resource arguments. */
  def collectWithResource[R, U: Coder](resource: => R, resourceType: ResourceType)(
    pfn: PartialFunction[(R, T, SideInputContext[T]), U]
  ): SCollectionWithSideInput[U] = {
    val o = coll
      .pApply(parDo(new CollectFnWithResource(resource, resourceType, pfn)))
      .setCoder(CoderMaterializer.beam(context, Coder[U]))
    new SCollectionWithSideInput[U](o, sides)
  }

  /** [[SCollection.batch]] that retains [[SideInput]]. */
  def batch(
    batchSize: Long,
    maxLiveWindows: Int = BatchDoFn.DEFAULT_MAX_LIVE_WINDOWS
  ): SCollectionWithSideInput[Iterable[T]] =
    new SCollectionWithSideInput[Iterable[T]](coll.batch(batchSize, maxLiveWindows), sides)

  /** [[SCollection.batchByteSized]] that retains [[SideInput]]. */
  def batchByteSized(
    batchByteSize: Long,
    maxLiveWindows: Int = BatchDoFn.DEFAULT_MAX_LIVE_WINDOWS
  ): SCollectionWithSideInput[Iterable[T]] =
    batchWeighted(batchByteSize, ScioUtil.elementByteSize(context), maxLiveWindows)

  /** [[SCollection.batchWeighted]] that retains [[SideInput]]. */
  def batchWeighted(
    batchWeight: Long,
    cost: T => Long,
    maxLiveWindows: Int = BatchDoFn.DEFAULT_MAX_LIVE_WINDOWS
  ): SCollectionWithSideInput[Iterable[T]] =
    new SCollectionWithSideInput[Iterable[T]](
      coll.batchWeighted(batchWeight, cost, maxLiveWindows),
      sides
    )

  /**
   * Allows multiple outputs from [[SCollectionWithSideInput]].
   *
   * @return
   *   map of side output to [[SCollection]]
   */
  private[values] def transformWithSideOutputs(
    sideOutputs: Seq[SideOutput[T]],
    name: String = "TransformWithSideOutputs"
  )(f: (T, SideInputContext[T]) => SideOutput[T]): Map[SideOutput[T], SCollection[T]] = {
    val _mainTag = SideOutput[T]()
    val tagToSide = sideOutputs.map(e => e.tupleTag.getId -> e).toMap +
      (_mainTag.tupleTag.getId -> _mainTag)

    val sideTags =
      TupleTagList.of(sideOutputs.map(e => e.tupleTag.asInstanceOf[TupleTag[_]]).asJava)

    def transformWithSideOutputsFn(
      partitions: Seq[SideOutput[T]],
      f: (T, SideInputContext[T]) => SideOutput[T]
    ): DoFn[T, T] =
      new SideInputDoFn[T, T] {
        val g = ClosureCleaner.clean(f) // defeat closure

        /*
         * ProcessContext is required as an argument because it is passed to SideInputContext
         * */
        @ProcessElement
        private[scio] def processElement(c: DoFn[T, T]#ProcessContext, w: BoundedWindow): Unit = {
          val elem = c.element()
          val partition = g(elem, sideInputContext(c, w))
          if (!partitions.exists(_.tupleTag == partition.tupleTag)) {
            throw new IllegalStateException(s"""${partition.tupleTag.getId} is not part of
            ${partitions.map(_.tupleTag.getId).mkString}""")
          }

          c.output(partition.tupleTag, elem)
        }
      }

    val transform = parDo[T, T](transformWithSideOutputsFn(sideOutputs, f))
      .withOutputTags(_mainTag.tupleTag, sideTags)

    val pCollectionWrapper = this.internal.apply(name, transform)
    pCollectionWrapper.getAll.asScala.view
      .mapValues(
        context
          .wrap(_)
          .asInstanceOf[SCollection[T]]
          .setCoder(internal.getCoder)
      )
      .flatMap { case (tt, col) => Try(tagToSide(tt.getId) -> col).toOption }
      .toMap
  }

  /** Convert back to a basic SCollection. */
  def toSCollection: SCollection[T] = coll
}
