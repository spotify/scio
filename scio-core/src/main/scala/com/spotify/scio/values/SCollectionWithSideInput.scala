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

import com.spotify.scio.util.FunctionsWithSideInput.SideInputDoFn
import com.spotify.scio.util.FunctionsWithSideInput
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{PCollection, TupleTag, TupleTagList}

import scala.jdk.CollectionConverters._
import scala.util.Try
import com.twitter.chill.ClosureCleaner

/**
 * An enhanced SCollection that provides access to one or more [[SideInput]]s for some transforms.
 * [[SideInput]]s are accessed via the additional [[SideInputContext]] argument.
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

  /**
   * Allows multiple outputs from [[SCollectionWithSideInput]].
   *
   * @return map of side output to [[SCollection]]
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
    pCollectionWrapper.getAll.asScala
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
