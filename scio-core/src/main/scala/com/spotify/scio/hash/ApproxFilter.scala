/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.hash

import java.io.{InputStream, OutputStream}

import com.google.common.{hash => g}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.twitter.{algebird => a}
import org.apache.beam.sdk.coders.{AtomicCoder, VarIntCoder, VarLongCoder}
import org.slf4j.LoggerFactory

/**
 * An approximate filter for instances of `T`, e.g. a Bloom filter. A Bloom filter offers an
 * approximate containment test with one-sided error: if it claims that an element is contained in
 * it, this might be in error, but if it claims that an element is not contained in it, then this
 * is definitely true.
 */
sealed trait ApproxFilter[T] extends Serializable {
  /**
   * Return `true` if the element might have been put in this filter, `false` if this is definitely
   * not the case.
   */
  def mightContain(elem: T): Boolean

  /**
   * Return an estimate for the total number of distinct elements that have been added to this
   * [[ApproxFilter]]. This approximation is reasonably accurate if it does not exceed the value of
   * `expectedInsertions` that was used when constructing the filter.
   */
  val approxElementCount: Long

  /**
   * Return the probability that [[mightContain]] will erroneously return `true` for an object
   * that has not actually been put in the [[ApproxFilter]].
   */
  val expectedFpp: Double
}

/**
 * Settings in case the elements need to be partitioned into multiple [[ApproxFilter]]s to
 * maintain the desired `fpp` and size.
 */
final case class PartitionSettings(partitions: Int, expectedInsertions: Long)

/** A trait for all [[ApproxFilter]] companion objects. */
sealed trait ApproxFilterCompanion {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Type of the hashing function for [[ApproxFilter]] elements, e.g. Guava
   * [[com.google.common.hash.Funnel Funnel]] or Algebird [[com.twitter.algebird.Hash128 Hash128]].
   */
  type Hash[T]

  /** Type of the [[ApproxFilter]] implementation. */
  type Filter[T] <: ApproxFilter[T]

  /**
   * Compute partition settings so that `expectedInsertions` can be done over one or more
   * [[ApproxFilter]]s while maintaining `fpp` and `maxBytes` in each filter.
   *
   * For example, when `expectedInsertions = 1L << 27` and `fpp = 0.01`, a Guava
   * [[com.google.common.hash.BloomFilter BloomFilter]] needs 1286484758 bits or 153MB, which
   * exceeds the default side input cache size in Dataflow.
   */
  def partitionSettings(expectedInsertions: Long, fpp: Double, maxBytes: Int): PartitionSettings

  // for Scala collections

  /**
   * Creates an [[ApproxFilter]] from an [[Iterable]] with the collection size as
   * `expectedInsertions` and default `fpp` of 0.03.
   *
   * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  final def create[T: Hash](elems: Iterable[T]): Filter[T] =
    create(elems, elems.size)

  /**
   * Creates an [[ApproxFilter]] from an [[Iterable]] with the expected number of insertions and
   * default `fpp` of 0.03.
   *
   * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  final def create[T: Hash](elems: Iterable[T], expectedInsertions: Long): Filter[T] =
    create(elems, expectedInsertions, 0.03)

  /**
   * Creates an [[ApproxFilter]] from an [[Iterable]] with the expected number of insertions and
   * expected false positive probability.
   *
   * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  final def create[T: Hash](
    elems: Iterable[T],
    expectedInsertions: Long,
    fpp: Double
  ): Filter[T] = {
    val filter = createImpl(elems, expectedInsertions, fpp)
    if (filter.approxElementCount > expectedInsertions) {
      logger.warn(
        "Approximate element count exceeds expected, {} > {}",
        filter.approxElementCount,
        expectedInsertions
      )
    }
    if (filter.expectedFpp > fpp) {
      logger.warn("False positive probability exceeds expected, {}} > {}", filter.expectedFpp, fpp)
    }
    filter
  }

  protected def createImpl[T: Hash](
    elems: Iterable[T],
    expectedInsertions: Long,
    fpp: Double
  ): Filter[T]

  // for SCollection, naive implementation with group-all

  /**
   * [[Coder]] for the [[ApproxFilter]] implementation.
   *
   * Note that [[Hash]] should be supplied at compile time and not serialized since it might not
   * have deterministic serialization.
   */
  implicit def coder[T: Hash]: Coder[Filter[T]]

  /**
   * Creates an [[ApproxFilter]] from an [[SCollection]] with the collection size as
   * `expectedInsertions` and default `fpp` of 0.03.
   *
   * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  final def create[T: Hash](elems: SCollection[T]): SCollection[Filter[T]] =
    // size is unknown, count after groupBy
    create(elems, 0)

  /**
   * Creates an [[ApproxFilter]] from an [[SCollection]] with the expected number of insertions and
   * default `fpp` of 0.03.
   *
   * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  final def create[T: Hash](
    elems: SCollection[T],
    expectedInsertions: Long
  ): SCollection[Filter[T]] =
    create(elems, expectedInsertions, 0.03)

  /**
   * Creates an [[ApproxFilter]] from an [[SCollection]] with the expected number of insertions and
   * expected false positive probability.
   *
   * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  final def create[T: Hash](
    elems: SCollection[T],
    expectedInsertions: Long,
    fpp: Double
  ): SCollection[Filter[T]] = {
    implicit val elemCoder = Coder.beam(elems.internal.getCoder)
    elems.transform {
      _.groupBy(_ => ()).values
        .map { xs =>
          val n = if (expectedInsertions > 0) expectedInsertions else xs.size
          create(xs, n, fpp)
        }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
// Guava Bloom Filter
////////////////////////////////////////////////////////////////////////////////

/**
 * An [[ApproxFilter]] implementation backed by a Guava
 * [[com.google.common.hash.BloomFilter BloomFilter]].
 *
 * Import `magnolify.guava.auto._` to get common instances of Guava
 * [[com.google.common.hash.Funnel Funnel]]s.
 */
class BloomFilter[T: g.Funnel] private (private val impl: g.BloomFilter[T])
    extends ApproxFilter[T] {
  override def mightContain(elem: T): Boolean = impl.mightContain(elem)
  override val approxElementCount: Long = impl.approximateElementCount()
  override val expectedFpp: Double = impl.expectedFpp()
}

/**  Companion object for [[BloomFilter]]. */
object BloomFilter extends ApproxFilterCompanion {
  override type Hash[T] = g.Funnel[T]
  override type Filter[T] = BloomFilter[T]

  override def partitionSettings(expectedInsertions: Long, fpp: Double, maxBytes: Int): PartitionSettings = {
    // empirical scaling factor to ensure the filter is not completely saturated
    // see [[BF.contains]]
    val n = (expectedInsertions * 1.1).toLong

    // see [[BloomFilter.optimalNumOfBits]]
    val optimalNumOfBits =
      (-n * 1.1 * math.log(fpp) / (math.log(2) * math.log(2))).toLong

    // given a constant fpp, optimalNumOfBits scales linearly with expectedInsertions
    val maxBits = maxBytes.toLong * 8
    val partitions = math.ceil(optimalNumOfBits.toDouble / maxBits).toInt
    val capacity = math.ceil(n.toDouble / partitions).toLong

    PartitionSettings(partitions, capacity)
  }

  private class BloomFilterCoder[T](implicit val hash: Hash[T]) extends AtomicCoder[Filter[T]] {
    override def encode(value: Filter[T], outStream: OutputStream): Unit =
      value.impl.writeTo(outStream)
    override def decode(inStream: InputStream): Filter[T] =
      new BloomFilter[T](g.BloomFilter.readFrom(inStream, hash))
  }

  implicit override def coder[T: Hash]: Coder[Filter[T]] = Coder.beam(new BloomFilterCoder[T]())

  override protected def createImpl[T: Hash](
    elems: Iterable[T],
    expectedInsertions: Long,
    fpp: Double
  ): Filter[T] = {
    val hash = implicitly[Hash[T]]
    val impl = g.BloomFilter.create(hash, expectedInsertions, fpp)
    elems.foreach(impl.put)
    new BloomFilter[T](impl)
  }
}

////////////////////////////////////////////////////////////////////////////////
// Algebird Bloom Filter
////////////////////////////////////////////////////////////////////////////////

/**
 * An [[ApproxFilter]] implementation backed by an Algebird
 * [[com.twitter.algebird.BloomFilter BloomFilter]].
 */
class ABloomFilter[T: a.Hash128] private (private val impl: a.BF[T]) extends ApproxFilter[T] {
  override def mightContain(elem: T): Boolean = impl.maybeContains(elem)
  override val approxElementCount: Long = impl.size.estimate
  override val expectedFpp: Double = if (impl.density > 0.95) {
    1.0
  } else {
    math.pow(1 - math.exp(-impl.numHashes * approxElementCount * 1.1 / impl.width), impl.numHashes)
  }
}

/**  Companion object for [[ABloomFilter]]. */
object ABloomFilter extends ApproxFilterCompanion {
  override type Hash[T] = a.Hash128[T]
  override type Filter[T] = ABloomFilter[T]

  override def partitionSettings(expectedInsertions: Long, fpp: Double, maxBytes: Int): PartitionSettings = {
    // at fpp = 0.01, double to int rounding error happens when numEntries > (1 << 27)
    // e.g.:
    // let e(n) = estimateWith(1<<n, 0.01).toDouble
    // e(27) / e(26) == 1.999999998445376 ~= 2.0
    // e(28) / e(27) == 1.6692647401973613 < 2.0
    // also, e(27) = 1286484759 bits = 153MB, hitting the limit of side input cache
    // set numEntries upper bound to 1 << 27 to avoid high false positive
    def estimateWidth(numEntries: Int, fpProb: Double): Int =
      math
        .ceil(-numEntries * math.log(fpProb) / math.log(2) / math.log(2))
        .toInt

    // upper bound of n as 2^x, e.g. upper(128) == 128, upper(129) == 256
    def upper(n: Int): Int = 1 << (0 to 27).find(1 << _ >= n).get

    // cap capacity between [minSize, maxSize] and find upper bound of 2^x
    val (minSize, maxSize) = (2048, 1 << 27)
    var capacity = upper(math.max(math.min(expectedInsertions, maxSize).toInt, minSize))

    // find a width with the given capacity
    var width = estimateWidth(capacity, fpp)
    while (width == Int.MaxValue) {
      capacity = capacity >> 1
      width = estimateWidth(capacity, fpp)
    }

    // see [[BF.contains]] about the 1.1 factor
    val partitions = math.ceil(expectedInsertions * 1.1 / capacity).toInt
    PartitionSettings(partitions, capacity)
  }

  // naive implementation, encodes all 4 instances, e.g. BFZero, BFItem, BFSparse, BFInstance as
  // dense bit set
  private class ABloomFilterCoder[T: Hash] extends AtomicCoder[Filter[T]] {
    private val intCoder = VarIntCoder.of()
    private val longCoder = VarLongCoder.of()

    override def encode(value: ABloomFilter[T], outStream: OutputStream): Unit = {
      intCoder.encode(value.impl.numHashes, outStream)
      intCoder.encode(value.impl.width, outStream)

      val bits = value.impl.toBitSet.toBitMask
      intCoder.encode(bits.length, outStream)
      var i = 0
      while (i < bits.length) {
        longCoder.encode(bits(i), outStream)
        i += 1
      }
    }

    override def decode(inStream: InputStream): ABloomFilter[T] = {
      val numHashes = intCoder.decode(inStream)
      val width = intCoder.decode(inStream)
      val hashes = a.BFHash(numHashes, width)

      val n = intCoder.decode(inStream)
      val bits = new Array[Long](n)
      var i = 0
      while (i < n) {
        bits(i) = longCoder.decode(inStream)
        i += 1
      }
      val bitSet = scala.collection.immutable.BitSet.fromBitMaskNoCopy(bits)
      val impl = a.BFInstance(hashes, bitSet, width)
      new ABloomFilter[T](impl)
    }
  }

  implicit override def coder[T: Hash]: Coder[Filter[T]] = Coder.beam(new ABloomFilterCoder[T])

  override protected def createImpl[T: Hash](
    elems: Iterable[T],
    expectedInsertions: Long,
    fpp: Double
  ): Filter[T] = {
    require(expectedInsertions <= Int.MaxValue)
    val impl = a.BloomFilter(expectedInsertions.toInt, fpp).create(elems.iterator)
    new ABloomFilter[T](impl)
  }
}
