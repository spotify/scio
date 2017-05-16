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

import com.spotify.scio.{Implicits, ScioContext}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.{PCollectionTuple, TupleTag}
import org.joda.time.Instant

import scala.reflect.ClassTag

/** Encapsulate a side output for a transform. */
trait SideOutput[T] extends Serializable {
  private[scio] val tupleTag: TupleTag[T]
}

/** Companion object for [[SideOutput]]. */
object SideOutput {
  /** Create a new [[SideOutput]] instance. */
  def apply[T](): SideOutput[T] = new SideOutput[T] {
    override private[scio] val tupleTag: TupleTag[T] = new TupleTag[T]()
  }
}

/** Encapsulate context of one or more [[SideOutput]]s in an [[SCollectionWithSideOutput]]. */
class SideOutputContext[T] private[scio] (val context: DoFn[T, AnyRef]#ProcessContext) {
  /** Write a value to a given [[SideOutput]]. */
  def output[S](sideOutput: SideOutput[S],
                output: S,
                timestamp: Instant = null): SideOutputContext[T] = {
    if (timestamp == null) {
      context.output(sideOutput.tupleTag, output)
    } else {
      context.outputWithTimestamp(sideOutput.tupleTag, output, timestamp)
    }
    this
  }
}

/** Encapsulate output of one or more [[SideOutput]]s in an [[SCollectionWithSideOutput]]. */
class SideOutputCollections private[values] (private val tuple: PCollectionTuple,
                                             private val context: ScioContext) {
  import Implicits._

  /** Extract the [[SCollection]] of a given [[SideOutput]]. */
  def apply[T: ClassTag](sideOutput: SideOutput[T]): SCollection[T] = {
    val r = context.pipeline.getCoderRegistry
    val o = tuple.get(sideOutput.tupleTag).setCoder(r.getScalaCoder[T])
    context.wrap(o)
  }
}
