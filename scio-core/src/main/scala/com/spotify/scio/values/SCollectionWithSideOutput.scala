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
import com.spotify.scio.coders.{Coder, CoderMaterializer}

import com.spotify.scio.util.FunctionsWithSideOutput
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.{PCollection, TupleTag, TupleTagList}

import scala.collection.JavaConverters._

/**
 * An enhanced SCollection that provides access to one or more [[SideOutput]]s for some transforms.
 * [[SideOutput]]s are accessed via the additional [[SideOutputContext]] argument.
 * [[SCollection]]s of the [[SideOutput]]s are accessed via the additional
 * [[SideOutputCollections]] return value.
 */
class SCollectionWithSideOutput[T] private[values]
(val internal: PCollection[T],
 val context: ScioContext,
 sides: Iterable[SideOutput[_]])
  extends PCollectionWrapper[T] {

  private val sideTags = TupleTagList.of(sides.map(_.tupleTag).toList.asJava)

  /**
   * [[SCollection.flatMap]] with an additional [[SideOutputContext]] argument and additional
   * [[SideOutputCollections]] return value.
   */
  def flatMap[U: Coder](f: (T, SideOutputContext[T]) => TraversableOnce[U])
  : (SCollection[U], SideOutputCollections) = {
    val mainTag = new TupleTag[U]
    val tuple = this.applyInternal(
      ParDo.of(FunctionsWithSideOutput.flatMapFn(f)).withOutputTags(mainTag, sideTags))

    val main = tuple.get(mainTag).setCoder(CoderMaterializer.beam(context, Coder[U]))
    (context.wrap(main), new SideOutputCollections(tuple, context))
  }

  /**
   * [[SCollection.map]] with an additional [[SideOutputContext]] argument and additional
   * [[SideOutputCollections]] return value.
   */
  def map[U: Coder](f: (T, SideOutputContext[T]) => U)
  : (SCollection[U], SideOutputCollections) = {
    val mainTag = new TupleTag[U]
    val tuple = this.applyInternal(
      ParDo.of(FunctionsWithSideOutput.mapFn(f)).withOutputTags(mainTag, sideTags))

    val main = tuple.get(mainTag).setCoder(CoderMaterializer.beam(context, Coder[U]))
    (context.wrap(main), new SideOutputCollections(tuple, context))
  }

}
