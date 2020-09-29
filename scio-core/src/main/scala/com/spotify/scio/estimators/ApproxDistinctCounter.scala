package com.spotify.scio.estimators

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{PairSCollectionFunctions, SCollection}

/**
 * Approximate distinct element counter for type `T`, e.g. HyperLogLog or HyperLogLog++. This has two APIs one
 * estimate total distinct count for a given PCollection
 * @tparam T
 */
trait ApproxDistinctCounter[T] {

  /**
   * Return a SCollection with single (Long)value which is the estimated distinct count in the given SCollection with
   * type `T`
   */
  def estimateDistinctCount(in: SCollection[T]): SCollection[Long]

  /**
   * Approximate distinct element per each key in the given key value SCollection.
   * This will output estimated distinct count per each unique key.
   */
  def estimateDistinctCountPerKey[K](
    in: SCollection[(K, T)]
  )(implicit koder: Coder[K], voder: Coder[T]): SCollection[(K, Long)]
}
