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

import com.spotify.scio.values.SideInputContext
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

private[scio] object FunctionsWithSideInput {

  trait SideInputDoFn[T, U] extends NamedDoFn[T, U] {
    private var ctx: SideInputContext[T, U] = _
    def sideInputContext(c: DoFn[T, U]#ProcessContext): SideInputContext[T, U] = {
      if (ctx == null || ctx.context != c) {
        // Workaround for type inference limit
        ctx = new SideInputContext(c.asInstanceOf[DoFn[T, U]#ProcessContext])
      }
      ctx
    }
  }

  def filterFn[T](f: (T, SideInputContext[T, T]) => Boolean): DoFn[T, T] = new SideInputDoFn[T, T] {
    val g = ClosureCleaner(f)  // defeat closure
    @ProcessElement
    private[scio] def processElement(c: DoFn[T, T]#ProcessContext): Unit =
      if (g(c.element(), sideInputContext(c))) {
        c.output(c.element())
      }
  }

  def flatMapFn[T, U](f: (T, SideInputContext[T, U]) => TraversableOnce[U])
  : DoFn[T, U] = new SideInputDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    @ProcessElement
    private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
      val i = g(c.element(), sideInputContext(c)).toIterator
      while (i.hasNext) c.output(i.next())
    }
  }

  def mapFn[T, U](f: (T, SideInputContext[T, U]) => U): DoFn[T, U] = new SideInputDoFn[T, U] {
    val g = ClosureCleaner(f)  // defeat closure
    @ProcessElement
    private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit =
      c.output(g(c.element(), sideInputContext(c)))
  }

}
