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

package com.spotify.scio.transforms.syntax

import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms.{
  ParallelCollectFn,
  ParallelFilterFn,
  ParallelFlatMapFn,
  ParallelMapFn
}

trait SCollectionParallelismSyntax {

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with custom
   * parallelism, where `parallelism` is the number of concurrent `DoFn` threads per worker (default
   * to number of CPU cores).
   */
  implicit class CustomParallelismSCollection[T](private val self: SCollection[T]) {

    /**
     * Return a new SCollection by first applying a function to all elements of this SCollection,
     * and then flattening the results. `parallelism` is the number of concurrent `DoFn`s per
     * worker.
     * @group transform
     */
    def flatMapWithParallelism[U: Coder](
      parallelism: Int
    )(fn: T => TraversableOnce[U]): SCollection[U] =
      self.parDo(new ParallelFlatMapFn(parallelism)(fn))

    /**
     * Return a new SCollection containing only the elements that satisfy a predicate. `parallelism`
     * is the number of concurrent `DoFn`s per worker.
     * @group transform
     */
    def filterWithParallelism(
      parallelism: Int
    )(fn: T => Boolean): SCollection[T] =
      self.parDo(new ParallelFilterFn(parallelism)(fn))(self.coder)

    /**
     * Return a new SCollection by applying a function to all elements of this SCollection.
     * `parallelism` is the number of concurrent `DoFn`s per worker.
     * @group transform
     */
    def mapWithParallelism[U: Coder](parallelism: Int)(fn: T => U): SCollection[U] =
      self.parDo(new ParallelMapFn(parallelism)(fn))

    /**
     * Filter the elements for which the given `PartialFunction` is defined, and then map.
     * `parallelism` is the number of concurrent `DoFn`s per worker.
     * @group transform
     */
    def collectWithParallelism[U: Coder](
      parallelism: Int
    )(pfn: PartialFunction[T, U]): SCollection[U] =
      self.parDo(new ParallelCollectFn(parallelism)(pfn))
  }
}
