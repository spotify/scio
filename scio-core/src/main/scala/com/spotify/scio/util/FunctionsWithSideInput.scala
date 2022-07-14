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

import com.spotify.scio.values.SideInputContext
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import com.twitter.chill.ClosureCleaner

import scala.collection.compat._ // scalafix:ok

private[scio] object FunctionsWithSideInput {
  trait SideInputDoFn[T, U] extends NamedDoFn[T, U] {
    def sideInputContext(c: DoFn[T, U]#ProcessContext, w: BoundedWindow): SideInputContext[T] =
      // Workaround for type inference limit
      new SideInputContext(c.asInstanceOf[DoFn[T, AnyRef]#ProcessContext], w)
  }

  def filterFn[T](f: (T, SideInputContext[T]) => Boolean): DoFn[T, T] =
    new SideInputDoFn[T, T] {
      val g = ClosureCleaner.clean(f) // defeat closure
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, T]#ProcessContext, w: BoundedWindow): Unit =
        if (g(c.element(), sideInputContext(c, w))) {
          c.output(c.element())
        }
    }

  def flatMapFn[T, U](f: (T, SideInputContext[T]) => TraversableOnce[U]): DoFn[T, U] =
    new SideInputDoFn[T, U] {
      val g = ClosureCleaner.clean(f) // defeat closure
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, U]#ProcessContext, w: BoundedWindow): Unit = {
        val i = g(c.element(), sideInputContext(c, w)).iterator
        while (i.hasNext) c.output(i.next())
      }
    }

  def mapFn[T, U](f: (T, SideInputContext[T]) => U): DoFn[T, U] =
    new SideInputDoFn[T, U] {
      val g = ClosureCleaner.clean(f) // defeat closure
      @ProcessElement
      private[scio] def processElement(c: DoFn[T, U]#ProcessContext, w: BoundedWindow): Unit =
        c.output(g(c.element(), sideInputContext(c, w)))
    }
}
