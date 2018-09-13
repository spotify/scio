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
import com.spotify.scio.coders.Coder

import com.spotify.scio.util.FunctionsWithWindowedValue
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{Duration, Instant}

/** Window options for an [[SCollection]]. */
case class WindowOptions(trigger: Trigger = null,
                                             accumulationMode: AccumulationMode = null,
                                             allowedLateness: Duration = null,
                                             closingBehavior: ClosingBehavior = null,
                                             timestampCombiner: TimestampCombiner = null)

/** Value with window information to be used inside a [[WindowedSCollection]]. */
case class WindowedValue[T](value: T, timestamp: Instant, window: BoundedWindow, pane: PaneInfo) {

  /** Make a copy with new value. */
  def withValue[U](v: U): WindowedValue[U] = this.copy(value = v)

  /** Make a copy with new timestamp. */
  def withTimestamp(t: Instant): WindowedValue[T] = this.copy(timestamp = t)

  /** Make a copy with new window. */
  def withWindow(w: BoundedWindow): WindowedValue[T] = this.copy(window = w)

  /** Make a copy with new pane. */
  def withPane(p: PaneInfo): WindowedValue[T] = this.copy(pane = p)

}

/** An enhanced SCollection that provides access to window information via [[WindowedValue]]. */
class WindowedSCollection[T: Coder] private[values] (val internal: PCollection[T],
                                                        val context: ScioContext)
  extends PCollectionWrapper[T] {

  /** [[SCollection.filter]] with access to window information via [[WindowedValue]]. */
  def filter(f: WindowedValue[T] => Boolean): WindowedSCollection[T] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.filterFn(f)).internal, context)

  /** [[SCollection.flatMap]] with access to window information via [[WindowedValue]]. */
  def flatMap[U: Coder](f: WindowedValue[T] => TraversableOnce[WindowedValue[U]])
  : WindowedSCollection[U] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.flatMapFn(f)).internal, context)

  /** [[SCollection.keyBy]] with access to window information via [[WindowedValue]]. */
  def keyBy[K: Coder](f: WindowedValue[T] => K): WindowedSCollection[(K, T)] =
    this.map(wv => wv.copy(value = (f(wv), wv.value)))

  /** [[SCollection.map]] with access to window information via [[WindowedValue]]. */
  def map[U: Coder](f: WindowedValue[T] => WindowedValue[U]): WindowedSCollection[U] =
    new WindowedSCollection(this.parDo(FunctionsWithWindowedValue.mapFn(f)).internal, context)

  /** Convert back to a basic SCollection. */
  def toSCollection: SCollection[T] = context.wrap(internal)

}
