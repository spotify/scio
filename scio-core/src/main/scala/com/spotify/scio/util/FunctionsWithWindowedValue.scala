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

package com.spotify.scio.util

import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess
import com.spotify.scio.values.WindowedValue
import org.joda.time.Instant

private[scio] object FunctionsWithWindowedValue {

  abstract class WindowDoFn[T, U] extends DoFn[T, U] with RequiresWindowAccess

  def filterFn[T, U](f: WindowedValue[T] => Boolean): DoFn[T, T] = new WindowDoFn[T, T] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      val wv = WindowedValue(c.element(), c.timestamp(), c.window(), c.pane())
      if (g(wv)) c.output(c.element())
    }
  }

  def flatMapFn[T, U](f: WindowedValue[T] => TraversableOnce[WindowedValue[U]])
  : DoFn[T, U] = new WindowDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val wv = WindowedValue(c.element(), c.timestamp(), c.window(), c.pane())
      g(wv).foreach(v => c.outputWithTimestamp(v.value, v.timestamp))
    }
  }

  def mapFn[T, U](f: WindowedValue[T] => WindowedValue[U]): DoFn[T, U] = new WindowDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    override def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val wv = g(WindowedValue(c.element(), c.timestamp(), c.window(), c.pane()))
      c.outputWithTimestamp(wv.value, wv.timestamp)
    }
  }

}
