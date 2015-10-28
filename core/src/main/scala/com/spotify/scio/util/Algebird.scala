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
