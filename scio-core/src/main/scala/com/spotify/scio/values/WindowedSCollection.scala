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

import com.google.cloud.dataflow.sdk.transforms.windowing.Window.ClosingBehavior
import com.google.cloud.dataflow.sdk.transforms.windowing._
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode
import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.util.FunctionsWithWindowedValue
import org.joda.time.{Duration, Instant}

import scala.reflect.ClassTag

case class WindowOptions[W <: BoundedWindow](trigger: TriggerBuilder[W] = null,
                                             accumulationMode: AccumulationMode = null,
                                             allowedLateness: Duration = null,
                                             closingBehavior: ClosingBehavior = null,
                                             outputTimeFn: OutputTimeFn[BoundedWindow] = null)

case class WindowedValue[T](value: T, timestamp: Instant, window: BoundedWindow, pane: PaneInfo) {
  def withValue[U](v: U): WindowedValue[U] =
    WindowedValue(v, this.timestamp, this.window, this.pane)
}

class WindowedSCollection[T: ClassTag] private[values] (val internal: PCollection[T],
                                                        val context: ScioContext)
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  def filter(f: WindowedValue[T] => Boolean): WindowedSCollection[T] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.filterFn(f)).internal, context)

  def flatMap[U: ClassTag](f: WindowedValue[T] => TraversableOnce[WindowedValue[U]])
  : WindowedSCollection[U] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.flatMapFn(f)).internal, context)

  def keyBy[K: ClassTag](f: WindowedValue[T] => K): WindowedSCollection[(K, T)] =
    this.map(wv => wv.copy(value = (f(wv), wv.value)))

  def map[U: ClassTag](f: WindowedValue[T] => WindowedValue[U]): WindowedSCollection[U] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.mapFn(f)).internal, context)

  def toSCollection: SCollection[T] = context.wrap(internal)

}
