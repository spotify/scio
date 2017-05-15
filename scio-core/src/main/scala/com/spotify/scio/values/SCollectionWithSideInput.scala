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
import com.spotify.scio.util.FunctionsWithSideInput.SideInputDoFn
import com.spotify.scio.util.{ClosureCleaner, FunctionsWithSideInput}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{PCollection, TupleTag, TupleTagList}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try

/**
 * An enhanced SCollection that provides access to one or more [[SideInput]]s for some transforms.
 * [[SideInput]]s are accessed via the additional [[SideInputContext]] argument.
 */
class SCollectionWithSideInput[T: ClassTag] private[values] (val internal: PCollection[T],
                                                             val context: ScioContext,
                                                             sides: Iterable[SideInput[_]])
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  private def parDo[T, U](fn: DoFn[T, U]) = ParDo.of(fn).withSideInputs(sides.map(_.view).asJava)

  /** [[SCollection.filter]] with an additional [[SideInputContext]] argument. */
  def filter(f: (T, SideInputContext[T]) => Boolean): SCollectionWithSideInput[T] = {
    val o = this
      .pApply(parDo(FunctionsWithSideInput.filterFn(f)))
      .internal.setCoder(this.getCoder[T])
    new SCollectionWithSideInput[T](o, context, sides)
  }

  /** [[SCollection.flatMap]] with an additional [[SideInputContext]] argument. */
  def flatMap[U: ClassTag](f: (T, SideInputContext[T]) => TraversableOnce[U])
  : SCollectionWithSideInput[U] = {
    val o = this
      .pApply(parDo(FunctionsWithSideInput.flatMapFn(f)))
      .internal.setCoder(this.getCoder[U])
    new SCollectionWithSideInput[U](o, context, sides)
  }

  /** [[SCollection.keyBy]] with an additional [[SideInputContext]] argument. */
  def keyBy[K: ClassTag](f: (T, SideInputContext[T]) => K): SCollectionWithSideInput[(K, T)] =
    this.map((x, s) => (f(x, s), x))

  /** [[SCollection.map]] with an additional [[SideInputContext]] argument. */
  def map[U: ClassTag](f: (T, SideInputContext[T]) => U): SCollectionWithSideInput[U] = {
    val o = this
      .pApply(parDo(FunctionsWithSideInput.mapFn(f)))
      .internal.setCoder(this.getCoder[U])
    new SCollectionWithSideInput[U](o, context, sides)
  }

  /**
   * Allows multiple outputs from [[SCollectionWithSideInput]].
   *
   * @return map of side output to [[SCollection]]
   */
  private[values] def transformWithSideOutputs(sideOutputs: Seq[SideOutput[T]],
                                               name: String = "TransformWithSideOutputs")
                                              (f: (T, SideInputContext[T]) => SideOutput[T])
  : Map[SideOutput[T], SCollection[T]] = {
    val _mainTag = SideOutput[T]()
    val tagToSide = sideOutputs.map(e => e.tupleTag.getId -> e).toMap +
      (_mainTag.tupleTag.getId -> _mainTag)

    val sideTags = TupleTagList.of(sideOutputs.map(e =>
      e.tupleTag.asInstanceOf[TupleTag[_]]).asJava)

    def transformWithSideOutputsFn(partitions: Seq[SideOutput[T]],
                                   f: (T, SideInputContext[T]) => SideOutput[T])
    : DoFn[T, T] = new SideInputDoFn[T, T] {
      val g = ClosureCleaner(f) // defeat closure

      @ProcessElement
      private[scio] def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
        val elem = c.element()
        val partition = g(elem, sideInputContext(c))
        if (!partitions.exists(_.tupleTag == partition.tupleTag)) {
          throw new IllegalStateException(s"""${partition.tupleTag.getId} is not part of
            ${partitions.map(_.tupleTag.getId).mkString}""")
        }

        c.sideOutput(partition.tupleTag, elem)
      }
    }

    val transform = parDo
      .withOutputTags(_mainTag.tupleTag, sideTags)
      .of(transformWithSideOutputsFn(sideOutputs, f))

    val pCollectionWrapper = this.internal.apply(name, transform)
    pCollectionWrapper.getAll.asScala
      .mapValues(context.wrap(_).asInstanceOf[SCollection[T]].setCoder(internal.getCoder))
      .flatMap{ case(tt, col) => Try{tagToSide(tt.getId) -> col}.toOption }
      .toMap
  }

  /** Convert back to a basic SCollection. */
  def toSCollection: SCollection[T] = context.wrap(internal)

}
