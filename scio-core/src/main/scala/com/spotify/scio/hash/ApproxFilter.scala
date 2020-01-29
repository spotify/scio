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
    scala.math.pow(
      1 - scala.math.exp(-impl.numHashes * approxElementCount * 1.1 / impl.width),
      impl.numHashes
    )
  }
}

/**  Companion object for [[ABloomFilter]]. */
object ABloomFilter extends ApproxFilterCompanion {
  override type Hash[T] = a.Hash128[T]
  override type Filter[T] = ABloomFilter[T]

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
