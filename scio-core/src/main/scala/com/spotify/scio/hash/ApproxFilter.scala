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
import com.spotify.scio.values.{SCollection, SideInput}
import org.apache.beam.sdk.coders.AtomicCoder
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

  /**
   * Settings in case the elements need to be partitioned into multiple [[ApproxFilter]]s to
   * maintain the desired `fpp` and size.
   */
  final private[hash] case class PartitionSettings(
    partitions: Int,
    expectedInsertions: Long,
    sizeBytes: Long
  )

  /**
   * Compute partition settings so that `expectedInsertions` can be spread across one or more
   * [[ApproxFilter]]s while maintaining `fpp` and `maxBytes` in each filter.
   *
   * For example, when `expectedInsertions = 1L << 27` and `fpp = 0.01`, a Guava
   * [[com.google.common.hash.BloomFilter BloomFilter]] needs 1286484758 bits or 153MB, which
   * exceeds the default side input cache size in Dataflow.
   */
  private[hash] def partitionSettings(
    expectedInsertions: Long,
    fpp: Double,
    maxBytes: Int
  ): PartitionSettings

  //////////////////////////////
  // Scala collections
  //////////////////////////////

  /**
   * Creates an [[ApproxFilter]] from an [[Iterable]] with the collection size as
   * `expectedInsertions` and default `fpp` of 0.03.
   *
   * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  // Empirically (see [[com.twitter.algebird.BF.contains]])
  // `expectedInsertions` to number of unique elements ratio should be 1.1
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

  ////////////////////////////////////////////////////////////
  // for SCollection, naive implementation with group-all
  ////////////////////////////////////////////////////////////

  /**
   * [[Coder]] for the [[ApproxFilter]] implementation.
   *
   * Note that [[Hash]] should be supplied at compile time and not serialized since it might not
   * have deterministic serialization.
   */
  implicit def filterCoder[T: Hash]: Coder[Filter[T]]

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

  final private[scio] def createPartitionedSideInputs[T: Hash](
    elems: SCollection[T],
    expectedInsertions: Long,
    fpp: Double
  ): Seq[SideInput[Filter[T]]] =
    if (elems.context.isTest) {
      // use exact element count to avoid OOM from very large `expectedInsertions`
      Seq(
        create(elems, 0L, fpp)
          .asSingletonSideInput(create(Nil, 1L, fpp))
      )
    } else {
      val settings = partitionSettings(expectedInsertions, fpp, 100 * 1024 * 1024)
      logger.info(
        "Partition settings for approximate filter side input of {} keys: " +
          "partitions={}, expectedInsertions={}, sizeBytes={}",
        Seq(
          expectedInsertions,
          settings.partitions,
          settings.expectedInsertions,
          settings.sizeBytes
        )
      )
      elems
        .hashPartition(settings.partitions)
        .map { xs =>
          create(xs, settings.expectedInsertions, fpp)
            .asSingletonSideInput(create(Nil, 1L, fpp))
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

  override private[hash] def partitionSettings(
    expectedInsertions: Long,
    fpp: Double,
    maxBytes: Int
  ): PartitionSettings = {
    // see [[com.google.common.hash.BloomFilter.optimalNumOfBits]]
    def numBits(n: Long, p: Double) = (-n * math.log(p) / (math.log(2) * math.log(2))).toLong
    val optimalNumOfBits = numBits(expectedInsertions, fpp)

    // given a constant fpp, optimalNumOfBits scales linearly with expectedInsertions
    val maxBits = maxBytes.toLong * 8
    val partitions = math.ceil(optimalNumOfBits.toDouble / maxBits).toInt
    val capacity = math.ceil(expectedInsertions.toDouble / partitions).toLong

    PartitionSettings(partitions, capacity, numBits(capacity, fpp) / 8)
  }

  private class BloomFilterCoder[T](implicit val hash: Hash[T]) extends AtomicCoder[Filter[T]] {
    override def encode(value: Filter[T], outStream: OutputStream): Unit =
      value.impl.writeTo(outStream)
    override def decode(inStream: InputStream): Filter[T] =
      new BloomFilter[T](g.BloomFilter.readFrom(inStream, hash))
  }

  implicit override def filterCoder[T: Hash]: Coder[Filter[T]] =
    Coder.beam(new BloomFilterCoder[T]())

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
