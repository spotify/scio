package com.spotify.scio.hash

import com.google.common.{hash => g}

/**
 * A mutable, scalable wrapper around a Guava [[com.google.common.hash.BloomFilter BloomFilter]]
 *
 * Scalable bloom filters use a series of bloom filters, adding a new one and scaling its size by `growthRate`
 * once the previous filter is saturated in order to maintain the desired false positive probability `fpProb`.
 * A scalable bloom filter `contains` ("might contain") an item if any of its filters contains the item.
 *
 * Import `magnolify.guava.auto._` to get common instances of Guava [[com.google.common.hash.Funnel Funnel]]s.
 */
object MutableScalableBloomFilter {
  /**
   * The default parameter values for this implementation are based on the findings in "Scalable Bloom Filters",
   * Almeida, Baquero, et al.: http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
   *
   * @param initialCapacity   The capacity of the first filter
   * @param fpProb            The desired overall false positive probability
   * @param growthRate        The growth rate of each subsequent filter added to `filters`
   * @param tighteningRatio   The tightening ratio applied to the current `fpProb` to maintain the false positive probability over the sequence of filters
   * @tparam T                The type of objects inserted into the filter
   * @return  A scalable bloom filter
   */
  def apply[T: g.Funnel](
    initialCapacity: Long,
    fpProb: Double = 0.03,
    growthRate: Int = 2,
    tighteningRatio: Double = 0.9
  ): MutableScalableBloomFilter[T] =
    new MutableScalableBloomFilter(fpProb, initialCapacity, growthRate, tighteningRatio)
}

/**
 * @param fpProb            The desired false positive probability
 * @param headCapacity      The capacity of the filter at the head of `filters`
 * @param growthRate        The growth rate of each subsequent filter added to `filters`
 * @param tighteningRatio   The tightening ratio applied to the current `fpProb` to maintain the false positive probability over the sequence of filters
 * @param filters           The underlying 'plain' bloom filters
 * @param headCount         The number of items currently in the filter at the head of `filters`
 * @param funnel            The funnel to turn `T`s into bytes
 * @tparam T                The type of objects inserted into the filter
 */
class MutableScalableBloomFilter[T](
  var fpProb: Double,
  var headCapacity: Long,
  growthRate: Int,
  tighteningRatio: Double,
  var filters: List[g.BloomFilter[T]] = Nil,
  // storing a count of items in the head avoids calling the relatively expensive `approximateElementCount` after each insert
  var headCount: Long = 0L
)(implicit funnel: g.Funnel[T]) extends Serializable {
  def contains(item: T): Boolean = filters.exists(f => f.mightContain(item))
  def approximateElementCount: Long = filters.foldLeft(0L) { case (sum, filter) => sum + filter.approximateElementCount() }

  private def scale(): Unit = {
    val shouldGrow = headCount >= headCapacity || filters == Nil
    if (shouldGrow) {
      // on initial filter construction, leave fpProb or headCapacity at their initial values
      if (filters != Nil) {
        fpProb = fpProb * tighteningRatio
        headCapacity = growthRate * headCapacity
      }
      headCount = 0
      filters = g.BloomFilter.create[T](funnel, headCapacity, fpProb) :: filters
    }
  }

  def +(item: T): MutableScalableBloomFilter[T] = {
    scale()
    val changed = filters.head.put(item)
    if (changed) headCount = headCount + 1
    this
  }

  def ++(items: Iterable[T]): MutableScalableBloomFilter[T] = {
    items.foreach(i => this + i) // no bulk insert for guava BFs
    this
  }
}
