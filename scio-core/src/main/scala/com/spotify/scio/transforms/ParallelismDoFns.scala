/*
 * Copyright 2020 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.transforms

import com.spotify.scio.util.ParallelLimitedFn
import com.twitter.chill.ClosureCleaner
import org.apache.beam.sdk.transforms.DoFn

class ParallelCollectFn[T, U](parallelism: Int)(pfn: PartialFunction[T, U])
    extends ParallelLimitedFn[T, U](parallelism) {
  val isDefined: T => Boolean = ClosureCleaner.clean(pfn.isDefinedAt(_)) // defeat closure
  val g: PartialFunction[T, U] = ClosureCleaner.clean(pfn) // defeat closure
  def parallelProcessElement(c: DoFn[T, U]#ProcessContext): Unit =
    if (isDefined(c.element())) {
      c.output(g(c.element()))
    }
}

class ParallelFilterFn[T](parallelism: Int)(f: T => Boolean)
    extends ParallelLimitedFn[T, T](parallelism) {
  val g: T => Boolean = ClosureCleaner.clean(f) // defeat closure
  def parallelProcessElement(c: DoFn[T, T]#ProcessContext): Unit =
    if (g(c.element())) {
      c.output(c.element())
    }
}

class ParallelMapFn[T, U](parallelism: Int)(f: T => U)
    extends ParallelLimitedFn[T, U](parallelism) {
  val g: T => U = ClosureCleaner.clean(f) // defeat closure
  def parallelProcessElement(c: DoFn[T, U]#ProcessContext): Unit =
    c.output(g(c.element()))
}

class ParallelFlatMapFn[T, U](parallelism: Int)(f: T => TraversableOnce[U])
    extends ParallelLimitedFn[T, U](parallelism: Int) {
  val g: T => TraversableOnce[U] = ClosureCleaner.clean(f) // defeat closure
  def parallelProcessElement(c: DoFn[T, U]#ProcessContext): Unit = {
    val i = g(c.element()).toIterator
    while (i.hasNext) c.output(i.next())
  }
}
