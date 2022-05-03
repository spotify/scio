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

import org.apache.beam.sdk.transforms.DoFn
import com.spotify.scio.values.SideOutputContext
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import com.twitter.chill.ClosureCleaner

import scala.collection.compat._ // scalafix:ok

private[scio] object FunctionsWithSideOutput {
  trait SideOutputFn[T, U] extends NamedDoFn[T, U] {
    private var ctx: SideOutputContext[T] = _
    def sideOutputContext(c: DoFn[T, U]#ProcessContext): SideOutputContext[T] = {
      if (ctx == null || ctx.context != c) {
        // Workaround for type inference limit
        ctx = new SideOutputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext])
      }
      ctx
    }
  }

  def mapFn[T, U](f: (T, SideOutputContext[T]) => U): DoFn[T, U] =
    new SideOutputFn[T, U] {
      val g = ClosureCleaner.clean(f) // defeat closure
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit =
        c.output(g(c.element(), sideOutputContext(c)))
    }

  def flatMapFn[T, U](f: (T, SideOutputContext[T]) => TraversableOnce[U]): DoFn[T, U] =
    new SideOutputFn[T, U] {
      val g = ClosureCleaner.clean(f) // defeat closure
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
        val i = g(c.element(), sideOutputContext(c)).iterator
        while (i.hasNext) c.output(i.next())
      }
    }
}
