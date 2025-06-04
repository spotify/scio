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

package com.spotify.scio.hash

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.google.common.io.ByteStreams
import com.google.common.{hash => g}
import com.spotify.scio.coders.Coder

import scala.collection.compat._

/**
 * A mutable, scalable wrapper around a Guava [[com.google.common.hash.BloomFilter BloomFilter]].
 * Not thread-safe.
 *
 * Scalable bloom filters use a series of bloom filters, adding a new one and scaling its size by
 * `growthRate` once the previous filter is saturated. A scalable bloom filter `contains` ("might
 * contain") an item if any of its filters contains the item.
 *
 * `fpProb`, the initial false positive probability, and `tighteningRatio` must be carefully chosen
 * to maintain the desired overall false positive probability. Similarly, the `initialCapacity`
 * determines how often the SBF must scale to support a given capacity and influences the effective
 * false positive probability. See below for more details.
 *
 * Import `magnolify.guava.auto._` to get common instances of Guava
 * [[com.google.common.hash.Funnel Funnel]] s.
 *
 * When a SBF scales a new filter is appended. The false positive probability for a series of
 * `numFilters` appended filters is:
 * {{{
 * 1 - Range(0, numFilters).map { i => (1 - fpProb * scala.math.pow(tighteningRatio, i)) }.product
 * }}}
 *
 * For the defaults, the false positive probability after each append is 3%, 5.6%, 7.9%, 9.9%, 11.7%
 * and so on. It is therefore in the interest of the user to appropriately size the initial filter
 * so that the number of appends is small.
 *
 * An approximation of the long-term upper bound of the false positive probability is `fpProb / (1 -
 * tighteningRatio)`. For the defaults of `fpProb = 0.03` and `tighteningRatio = 0.9` this gives
 * `0.03 / (1 - 0.9)`, or around 30% false positives as an upper bound.
 *
 * Bloom filters inherently trade precision for space. For a single filter, the number of bits is:
 * {{{
 * -1 * capacity * scala.math.log(fpProb) / scala.math.pow(scala.math.log(2), 2)
 * }}}
 * For example, for an `initialCapacity` of 1 million and the default `fpProb` of 3%, a filter will
 * be 912 kilobytes; if `fpProb` is instead 0.1%, then the size grows to 1797 kilobytes.
 *
 * When using scalable bloom filters, if possible, always size the first filter to be larger than
 * the known number of items to be inserted rather than choosing a small default and letting the
 * filters scale to fit. For example, if a SBF must contain ~65.5 million items, then with the
 * defaults and an `initialCapacity` of 1000, the SBF will have scaled 16 times, will consume ~84
 * megabytes and have a false positive probability of ~22%. If, on the other hand, a SBF is
 * constructed with an initial capacity of 65.5 million items, it will not have scaled at all and
 * therefore have its initial false positive probability of 3%, and consume only ~60 megabytes.
 */
object MutableScalableBloomFilter {

  /**
   * The default parameter values for this implementation are based on the findings in "Scalable
   * Bloom Filters", Almeida, Baquero, et al.: http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
   *
   * Import `magnolify.guava.auto._` to get common instances of Guava
   * [[com.google.common.hash.Funnel Funnel]] s.
   *
   * @param initialCapacity
   *   The capacity of the first filter. Must be positive
   * @param fpProb
   *   The initial false positive probability
   * @param growthRate
   *   The growth rate of each subsequent filter added to `filters`
   * @param tighteningRatio
   *   The tightening ratio to be applied to the current `fpProb`
   * @tparam T
   *   The type of objects inserted into the filter
   * @return
   *   A scalable bloom filter
   */
  def apply[T: g.Funnel](
    initialCapacity: Long,
    fpProb: Double = 0.03,
    growthRate: Int = 2,
    tighteningRatio: Double = 0.9
  ): MutableScalableBloomFilter[T] = {
    require(initialCapacity > 0, "initialCapacity must be positive.")
    MutableScalableBloomFilter(
      fpProb,
      initialCapacity,
      growthRate,
      tighteningRatio,
      fpProb,
      0L,
      None,
      Nil
    )
  }

  def toBytes[T](sbf: MutableScalableBloomFilter[T]): Array[Byte] = {
    // serialize each of the fields, excepting the implicit funnel
    val baos = new ByteArrayOutputStream()
    val dos: DataOutputStream = new DataOutputStream(baos)

    dos.writeDouble(sbf.fpProb)
    dos.writeLong(sbf.headCapacity)
    dos.writeInt(sbf.growthRate)
    dos.writeDouble(sbf.tighteningRatio)
    dos.writeDouble(sbf.headFPProb)
    dos.writeLong(sbf.headCount)
    dos.writeInt(sbf.numFilters) // count of head + all tail filters
    sbf.head.foreach(filter => filter.writeTo(dos))
    sbf.tail.foreach {
      case Left(filter) => filter.writeTo(dos)
      case Right(ser)   => dos.write(ser.filterBytes)
    }
    baos.toByteArray
  }

  /**
   * Import `magnolify.guava.auto._` to get common instances of Guava
   * [[com.google.common.hash.Funnel Funnel]] s.
   */
  def fromBytes[T](
    bytes: Array[Byte]
  )(implicit funnel: g.Funnel[T]): MutableScalableBloomFilter[T] = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bais)

    val fpProb = dis.readDouble()
    val headCapacity = dis.readLong()
    val growthRate = dis.readInt()
    val tighteningRatio = dis.readDouble()
    val headFPProb = dis.readDouble()
    val headCount = dis.readLong()
    val numFilters = dis.readInt()
    val head = if (numFilters > 0) Some(g.BloomFilter.readFrom[T](dis, funnel)) else None
    val tail: List[Either[g.BloomFilter[T], SerializedBloomFilters]] = {
      if (numFilters > 1) {
        val baos = new ByteArrayOutputStream()
        ByteStreams.copy(dis, baos)
        List(Right(SerializedBloomFilters(numFilters - 1, baos.toByteArray)))
      } else {
        List.empty
      }
    }
    MutableScalableBloomFilter[T](
      fpProb,
      headCapacity,
      growthRate,
      tighteningRatio,
      headFPProb,
      headCount,
      head,
      tail
    )
  }

  implicit def coder[T](implicit funnel: g.Funnel[T]): Coder[MutableScalableBloomFilter[T]] =
    Coder.xmap[Array[Byte], MutableScalableBloomFilter[T]](Coder.arrayByteCoder)(
      bytes => fromBytes[T](bytes)(funnel),
      sbf => toBytes[T](sbf)
    )
}

case class SerializedBloomFilters(numFilters: Int, filterBytes: Array[Byte]) {
  def deserialize[T](implicit funnel: g.Funnel[T]): List[g.BloomFilter[T]] = {
    val bais = new ByteArrayInputStream(filterBytes)
    (1 to numFilters).map(_ => g.BloomFilter.readFrom[T](bais, funnel)).toList
  }
}

/**
 * Import `magnolify.guava.auto._` to get common instances of Guava
 * [[com.google.common.hash.Funnel Funnel]] s.
 *
 * @param fpProb
 *   The initial false positive probability
 * @param headCapacity
 *   The capacity of the filter at the head of `filters`
 * @param growthRate
 *   The growth rate of each subsequent filter added to `filters`
 * @param tighteningRatio
 *   The tightening ratio applied to `headFPProb` when scaling
 * @param head
 *   The underlying bloom filter currently being inserted into, or `None` if this scalable filter
 *   has just been initialized
 * @param tail
 *   The underlying already-saturated bloom filters, lazily deserialized from bytes as necessary.
 * @param headFPProb
 *   The false positive probability of the head of `filters`
 * @param headCount
 *   The number of items currently in the filter at the head of `filters`
 * @param funnel
 *   The funnel to turn `T`s into bytes
 * @tparam T
 *   The type of objects inserted into the filter
 */
case class MutableScalableBloomFilter[T](
  fpProb: Double,
  private var headCapacity: Long,
  private val growthRate: Int,
  private val tighteningRatio: Double,
  private var headFPProb: Double,
  // storing a count of items in the head avoids calling the relatively expensive `approximateElementCount` after each insert
  private var headCount: Long,
  private var head: Option[g.BloomFilter[T]],
  private var tail: List[Either[g.BloomFilter[T], SerializedBloomFilters]]
  // package private for testing purposes
)(implicit private val funnel: g.Funnel[T])
    extends Serializable {
  require(headCapacity > 0, "headCapacity must be positive.")

  // `SerializedBloomFilters` is never appended, so do deserialization check only once. package-private for testing.
  @transient private var deserialized = false
  private[hash] def deserialize(): List[g.BloomFilter[T]] = {
    if (!deserialized) {
      tail = tail.flatMap {
        case b @ Left(_) => List(b)
        case Right(ser)  => ser.deserialize(funnel).map(Left(_))
      }
      deserialized = true
    }
    tail.collect { case Left(bf) => bf }
  }

  /**
   * Note: Will cause deserialization of any `SerializedBloomFilters`.
   *
   * @param item
   *   The item to check
   * @return
   *   True if any of the backing filters 'might contain' `item`, false otherwise.
   */
  def mightContain(item: T): Boolean =
    head.exists(_.mightContain(item)) || deserialize().exists(f => f.mightContain(item))

  /**
   * Note: Will cause deserialization of any `SerializedBloomFilters`.
   *
   * @return
   *   The sum of the approximate element count for all underlying filters.
   */
  def approximateElementCount: Long =
    head.map(_.approximateElementCount()).getOrElse(0L) + deserialize()
      .map(_.approximateElementCount)
      .sum

  // for testing only
  private[hash] def numFilters: Int = {
    head.map(_ => 1).getOrElse(0) + tail.map {
      case Left(_)    => 1
      case Right(ser) => ser.numFilters
    }.sum
  }

  /** Scale the SBF and return the always-defined head filter */
  private def scale(): g.BloomFilter[T] = {
    // on construction of the first filter, leave headFPProb & headCapacity at their starting values
    head.foreach { h =>
      headFPProb = headFPProb * tighteningRatio
      headCapacity = growthRate * headCapacity
      tail = Left(h) :: tail
    }

    headCount = 0
    val newHead = g.BloomFilter.create[T](funnel, headCapacity, headFPProb)
    head = Some(newHead)
    newHead
  }

  def +=(item: T): MutableScalableBloomFilter[T] = {
    val headFilter = head match {
      case None    => scale()
      case Some(h) =>
        val shouldGrow = headCount >= headCapacity
        if (shouldGrow) scale() else h
    }
    val changed = headFilter.put(item)
    if (changed) headCount = headCount + 1
    this
  }

  def ++=(items: TraversableOnce[T]): MutableScalableBloomFilter[T] = {
    items.iterator.foreach(i => this += i) // no bulk insert for guava BFs
    this
  }
}
