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

package com.spotify.scio.values

import java.io._

import com.google.common.hash.{Funnel, BloomFilter => gBloomFilter}
import com.spotify.scio.annotations.experimental

/**
 * Bloom Filter - a probabilistic data structure to test approximate presence of an element.
 *
 * Operations
 * 1) create: Hash the value k times, updating the bitfield at
 *            the index equal to each hashed value
 * 2) query: hash the value k times.  If there are k collisions, then return true; otherwise false.
 *
 * http://en.wikipedia.org/wiki/Bloom_filter
 *
 * Implemented as an immutable wrapper over Guava's Bloom Filter.
 */
@SerialVersionUID(1L)
final case class BloomFilter[T] private (
  private val internal: gBloomFilter[T],
  private val funnel: Funnel[T]
) extends ApproxFilter[T] {

  /**
   * Returns the probability that [[mightContain(t: T)]] will erroneously return `true`
   * for an element that was not actually present the colleciton from which this [[BloomFilter]]
   * was built.
   *
   * Ideally, this number should be close to the `fpProb` parameter passed to the [[BloomFilter#apply]]
   * when this filter was built or smaller. If it is significantly higher, it is usually the
   * case that too many elements (more than expected) were present in the original collection
   * from which this [[BloomFilter]] was built.
   */
  val expectedFpp: Double = internal.expectedFpp()

  /**
   * Returns an estimate for the total number of distinct elements that have been added to this
   * [[BloomFilter]]. This approximation is reasonably accurate if it does not exceed the value of
   * `expectedInsertions` that was used when constructing the filter.
   */
  val approximateElementCount: Long = internal.approximateElementCount()

  /**
   * Check approximate presence of an element in the BloomFilter
   *
   * @return true if the element may be present, false if it is definitely not present.
   */
  def mightContain(t: T): Boolean = internal.mightContain(t)

  // Java Serialization
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(funnel)
    internal.writeTo(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    val funnel = in.readObject().asInstanceOf[Funnel[T]]
    val internal = gBloomFilter.readFrom(in, funnel)
    setField("funnel", funnel)
    setField("internal", internal)
  }

  /**
   * Add an element to this [[BloomFilter]]. It creates a copy of the underlying
   * structure.
   *
   * For creating BloomFilters from large collections, use [[BloomFilter#apply(Iterable)]]
   * instead.
   */
  @experimental
  def put(t: T): BloomFilter[T] = {
    val copyOfInternal = internal.copy()
    copyOfInternal.put(t)
    this.copy(internal = copyOfInternal)
  }

  /**
   * Add an Itearable of elements to this [[BloomFilter]]. It creates a copy of the underlying
   * structure.
   *
   * For creating BloomFilters from large collections, use [[BloomFilter#apply(Iterable)]]
   * instead.
   */
  @experimental
  def putAll(elements: Iterable[T]): BloomFilter[T] = {
    val copyOfInternal = internal.copy()
    val it = elements.iterator
    while (it.hasNext) {
      copyOfInternal.put(it.next())
    }
    this.copy(internal = copyOfInternal)
  }
}

object BloomFilter {

  /**
   * Constructor for [[BloomFilter]]
   *
   * @param iterable An iterable of elements to be stored in the BloomFilter
   * @param fpProb allowed false positive probability
   * @param f [[Funnel]] for the type [[T]]
   * @return A [[BloomFilter]] for the given [[Iterable]] of elements.
   */
  def apply[T](
    iterable: Iterable[T],
    estimatedNumElements: Int,
    fpProb: Double
  )(
    implicit f: Funnel[T]
  ): BloomFilter[T] = {
    val settings = BloomFilter.optimalBFSettings(estimatedNumElements, fpProb)
    require(
      settings.numBFs == 1,
      s"BloomFilter overflow: $estimatedNumElements elements found, max allowed: ${settings.capacity}"
    )

    val bf = gBloomFilter.create[T](f, estimatedNumElements, fpProb)
    val it = iterable.iterator
    while (it.hasNext) {
      bf.put(it.next())
    }
    BloomFilter(bf, f)
  }

  /**
   * Constructor for an empty [[BloomFilter]]
   *
   * An empty BloomFilter is particularly useful when used with
   * [[SCollection.asSetSingletonSideInput(defaultValue)]]
   *
   * @param numElements The number of elements that we might later want to insert using
   *                    [[BloomFilter.put]] to create a new [[BloomFilter]]
   * @param fpProb Expected false positive probability of the resulting [[BloomFilter]]
   *               Default 0.01 (1 %)
   */
  def empty[T: Funnel](numElements: Int, fpProb: Double = 0.01): BloomFilter[T] = {
    val f = implicitly[Funnel[T]]
    BloomFilter(gBloomFilter.create[T](f, numElements, fpProb), f)
  }

  // ************************************************************************
  // Private helpers for constructing BloomFilters.
  // ************************************************************************

  private[values] final case class BFSettings(width: Int, capacity: Int, numBFs: Int)

  /*
   * This function calculates the width and number of bloom filters that would be optimally
   * required to maintain the given fpProb.
   *
   * TODO reuse this in [[PairSCollectionFunctions]]
   */
  private[values] def optimalBFSettings(numEntries: Long, fpProb: Double): BFSettings = {
    // double to int rounding error happens when numEntries > (1 << 27)
    // set numEntries upper bound to 1 << 27 to avoid high false positive
    def estimateWidth(numEntries: Int, fpProb: Double): Int =
      math
        .ceil(-1 * numEntries * math.log(fpProb) / math.log(2) / math.log(2))
        .toInt

    // upper bound of n as 2^x
    def upper(n: Int): Int = 1 << (0 to 27).find(1 << _ >= n).get

    // cap capacity between [minSize, maxSize] and find upper bound of 2^x
    val (minSize, maxSize) = (2048, 1 << 27)
    var capacity = upper(math.max(math.min(numEntries, maxSize).toInt, minSize))

    // find a width with the given capacity
    var width = estimateWidth(capacity, fpProb)
    while (width == Int.MaxValue) {
      capacity = capacity >> 1
      width = estimateWidth(capacity, fpProb)
    }
    val numBFs = (numEntries / capacity).toInt + 1

    val totalBytes = width.toLong * numBFs / 8
    val totalSizeMb = totalBytes / 1024.0 / 1024.0

    BFSettings(width, capacity, numBFs)
  }
}
