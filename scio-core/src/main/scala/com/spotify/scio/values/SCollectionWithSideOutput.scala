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

import com.spotify.scio.util.FunctionsWithSideOutput
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{PCollection, TupleTag, TupleTagList}

import scala.jdk.CollectionConverters._

/**
 * An enhanced SCollection that provides access to one or more [[SideOutput]] s for some transforms.
 * [[SideOutput]] s are accessed via the additional [[SideOutputContext]] argument. [[SCollection]]
 * s of the [[SideOutput]] s are accessed via the additional [[SideOutputCollections]] return value.
 */
class SCollectionWithSideOutput[T] private[values] (
  coll: SCollection[T],
  sides: Iterable[SideOutput[_]]
) extends PCollectionWrapper[T] {

  override val internal: PCollection[T] = coll.internal
  override val context: ScioContext = coll.context

  private val sideTags = TupleTagList.of(sides.map(_.tupleTag).toList.asJava)

  override def withName(name: String): this.type = {
    coll.withName(name)
    this
  }

  private def apply[U: Coder](f: DoFn[T, U]): (SCollection[U], SideOutputCollections) = {
    val mainTag = new TupleTag[U]

    val dofn = ParDo.of(f).withOutputTags(mainTag, sideTags)
    val tuple = this.applyInternal(coll.tfName, dofn)

    val main = tuple.get(mainTag).setCoder(CoderMaterializer.beam(context, Coder[U]))

    sides.foreach { s =>
      tuple
        .get(s.tupleTag)
        // The compiler can't unify the type of the PCollection returned by `get`
        // with the type of the coder in SideOutput. We just force everything to `Any`.
        .asInstanceOf[PCollection[Any]]
        .setCoder(CoderMaterializer.beam(context, s.coder.asInstanceOf[Coder[Any]]))
    }

    (context.wrap(main), new SideOutputCollections(tuple, context))
  }

  /**
   * [[SCollection.flatMap]] with an additional [[SideOutputContext]] argument and additional
   * [[SideOutputCollections]] return value.
   */
  def flatMap[U: Coder](
    f: (T, SideOutputContext[T]) => TraversableOnce[U]
  ): (SCollection[U], SideOutputCollections) =
    apply[U](FunctionsWithSideOutput.flatMapFn(f))

  /**
   * [[SCollection.map]] with an additional [[SideOutputContext]] argument and additional
   * [[SideOutputCollections]] return value.
   */
  def map[U: Coder](f: (T, SideOutputContext[T]) => U): (SCollection[U], SideOutputCollections) =
    apply[U](FunctionsWithSideOutput.mapFn(f))
}
