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

package com.spotify.scio.util

import com.spotify.scio.values.WindowedValue
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import com.twitter.chill.ClosureCleaner

import scala.collection.compat._ // scalafix:ok

private[scio] object FunctionsWithWindowedValue {
  def filterFn[T, U](f: WindowedValue[T] => Boolean): DoFn[T, T] =
    new NamedDoFn[T, T] {
      val g = ClosureCleaner.clean(f) // defeat closure
      @ProcessElement
      private[scio] def processElement(
        c: DoFn[T, T]#ProcessContext,
        window: BoundedWindow
      ): Unit = {
        val wv = WindowedValue(c.element(), c.timestamp(), window, c.pane())
        if (g(wv)) c.output(c.element())
      }
    }

  def flatMapFn[T, U](f: WindowedValue[T] => TraversableOnce[WindowedValue[U]]): DoFn[T, U] =
    new NamedDoFn[T, U] {
      val g = ClosureCleaner.clean(f) // defeat closure
      @ProcessElement
      private[scio] def processElement(
        c: DoFn[T, U]#ProcessContext,
        window: BoundedWindow
      ): Unit = {
        val wv = WindowedValue(c.element(), c.timestamp(), window, c.pane())
        val i = g(wv).iterator
        while (i.hasNext) {
          val v = i.next()
          c.outputWithTimestamp(v.value, v.timestamp)
        }
      }
    }

  def mapFn[T, U](f: WindowedValue[T] => WindowedValue[U]): DoFn[T, U] =
    new NamedDoFn[T, U] {
      val g = ClosureCleaner.clean(f) // defeat closure
      @ProcessElement
      private[scio] def processElement(
        c: DoFn[T, U]#ProcessContext,
        window: BoundedWindow
      ): Unit = {
        val wv = g(WindowedValue(c.element(), c.timestamp(), window, c.pane()))
        c.outputWithTimestamp(wv.value, wv.timestamp)
      }
    }
}
