/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio.util

import com.twitter.algebird.{Aggregator, MonoidAggregator, TopK, TopKMonoid}

/** Utility for Algebird. */
object Algebird {

  /**
   * Similar to [[com.twitter.algebird.Aggregator.sortedTake]] but immutable.
   */
  def sortedTake[T: Ordering](count: Int): MonoidAggregator[T, TopK[T], Seq[T]] =
    topKAggregator(count)

  /**
   * Similar to [[com.twitter.algebird.Aggregator.sortedReverseTake]] but immutable.
   */
  def sortedReverseTake[T: Ordering](count: Int): MonoidAggregator[T, TopK[T], Seq[T]] =
    topKAggregator(count)(implicitly[Ordering[T]].reverse)

  private def topKAggregator[T](max: Int)(implicit ord: Ordering[T]) =
    Aggregator
      .prepareMonoid[T, TopK[T]](i => new TopK(1, List(i), Some(i)))(new TopKMonoid[T](max)(ord))
      .andThenPresent(_.items.toSeq)

}
