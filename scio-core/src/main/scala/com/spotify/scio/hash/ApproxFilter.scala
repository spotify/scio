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

sealed trait ApproxFilter[T] extends Serializable {
  def mightContain(elem: T): Boolean

  val approxElementCount: Long
  val expectedFpp: Double
}

sealed trait ApproxFilterCompanion {
  private val logger = LoggerFactory.getLogger(this.getClass)

  type Hash[T]
  type Filter[T] <: ApproxFilter[T]

  // for Scala collections

  final def create[T: Hash](elems: Iterable[T]): Filter[T] =
    create(elems, elems.size)

  final def create[T: Hash](elems: Iterable[T], expectedInsertions: Long): Filter[T] =
    create(elems, expectedInsertions, 0.03)

  final def create[T: Hash](elems: Iterable[T], expectedInsertions: Long, fpp: Double): Filter[T] = {
    val filter = createImpl(elems, expectedInsertions, fpp)
    if (filter.approxElementCount > expectedInsertions) {
      logger.warn("Approximate element count exceeds expected, {} > {}",
        filter.approxElementCount, expectedInsertions)
    }
    if (filter.expectedFpp > fpp) {
      logger.warn("False positive probability exceeds expected, {}} > {}", filter.expectedFpp, fpp)
    }
    filter
  }

  protected def createImpl[T: Hash](elems: Iterable[T], expectedInsertions: Long, fpp: Double): Filter[T]

  // for SCollection, naive implementation with group-all

  implicit def coder[T: Hash]: Coder[Filter[T]]

  final def create[T: Hash](elems: SCollection[T]): SCollection[Filter[T]] =
    create(elems, 0)

  final def create[T: Hash](elems: SCollection[T], expectedInsertions: Long): SCollection[Filter[T]] =
    create(elems, expectedInsertions, 0.03)

  def create[T: Hash](elems: SCollection[T], expectedInsertions: Long, fpp: Double): SCollection[Filter[T]] = {
    implicit val elemCoder = Coder.beam(elems.internal.getCoder)
    elems.transform { _
      .groupBy(_ => ())
      .values
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

class BloomFilter[T: g.Funnel] private (private val impl: g.BloomFilter[T]) extends ApproxFilter[T] {
  override def mightContain(elem: T): Boolean = impl.mightContain(elem)
  override val approxElementCount: Long = impl.approximateElementCount()
  override val expectedFpp: Double = impl.expectedFpp()
}

object BloomFilter extends ApproxFilterCompanion {
  override type Hash[T] = g.Funnel[T]
  override type Filter[T] = BloomFilter[T]

  private class BloomFilterCoder[T](implicit val hash: Hash[T])
    extends AtomicCoder[Filter[T]] {
    override def encode(value: Filter[T], outStream: OutputStream): Unit =
      value.impl.writeTo(outStream)
    override def decode(inStream: InputStream): Filter[T] =
      new BloomFilter[T](g.BloomFilter.readFrom(inStream, hash))
  }

  override implicit def coder[T: Hash]: Coder[Filter[T]] = Coder.beam(new BloomFilterCoder[T]())

  protected override def createImpl[T: Hash](elems: Iterable[T], expectedInsertions: Long, fpp: Double): Filter[T] = {
    val hash = implicitly[Hash[T]]
    val impl = g.BloomFilter.create(hash, expectedInsertions, fpp)
    elems.foreach(impl.put)
    new BloomFilter[T](impl)
  }
}

////////////////////////////////////////////////////////////////////////////////
// Algebird Bloom Filter
////////////////////////////////////////////////////////////////////////////////

class ABloomFilter[T: a.Hash128] private (private val impl: a.BF[T]) extends ApproxFilter[T] {
  override def mightContain(elem: T): Boolean = impl.maybeContains(elem)
  override val approxElementCount: Long = impl.size.estimate
  override val expectedFpp: Double = if (impl.density > 0.95) {
    1.0
  } else {
    scala.math.pow(1 - scala.math.exp(-impl.numHashes * approxElementCount * 1.1 / impl.width), impl.numHashes)
  }
}

object ABloomFilter extends ApproxFilterCompanion {
  override type Hash[T] = a.Hash128[T]
  override type Filter[T] = ABloomFilter[T]

  // naive implementation, encodes BFZero, BFItem, BFSparse as BFInstance with dense bit set
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

  override implicit def coder[T: Hash]: Coder[Filter[T]] = Coder.beam(new ABloomFilterCoder[T])

  protected override def createImpl[T: Hash](elems: Iterable[T], expectedInsertions: Long, fpp: Double): Filter[T] = {
    require(expectedInsertions <= Int.MaxValue)
    val impl = a.BloomFilter(expectedInsertions.toInt, fpp).create(elems.iterator)
    new ABloomFilter[T](impl)
  }
}
