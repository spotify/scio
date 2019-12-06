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

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

import com.google.common.hash.{Funnel, BloomFilter => gBloomFilter}
import com.spotify.scio.coders.Coder

import scala.collection.mutable

@SerialVersionUID(1L)
final case class ScalableBloomFilter[T] private (
  // When changing the order of the types, change the implicit Coder in companion object
  fpProb: Double,
  initialCapacity: Int,
  growthRate: Int,
  tighteningRatio: Double,
  private val filters: ::[gBloomFilter[T]] // This is a NonEmptyList
) extends ApproxFilter[T] {
  /**
   * Check if the filter may contain a given element.
   */
  override def mayBeContains(t: T): Boolean = filters.exists(_.mightContain(t))

  def numFilters: Int = filters.size // Explain why min in always 1

  def approximateElementCount: Long = filters.map(_.approximateElementCount()).sum

  /**
   * Add more elements, and create a new [[ScalableBloomFilter]]
   */
  def putAll(
    moreElements: Iterable[T]
  )(implicit funnel: Funnel[T]): ScalableBloomFilter[T] = {
    val initial: ScalableBloomFilter[T] = this

    var numInsertedInCurrentFilter = initial.filters.head.approximateElementCount()
    var currentCapacity = initial.initialCapacity * (growthRate * initial.numFilters)
    var currentFpProb = initial.fpProb * (tighteningRatio * initial.numFilters)

    val it = moreElements.iterator
    // create a copy
    val newFilters = initial.filters.to[mutable.ListBuffer]

    while (it.hasNext) {
      while (it.hasNext && numInsertedInCurrentFilter < currentCapacity) {
        newFilters.head.put(it.next())
        numInsertedInCurrentFilter += 1
      }

      if (it.hasNext) {
        // We have more elements to insert
        currentCapacity *= growthRate
        currentFpProb *= tighteningRatio
        numInsertedInCurrentFilter = 0
        val f = gBloomFilter.create[T](funnel, currentCapacity, currentFpProb)
        newFilters.insert(0, f)
      }
    }

    initial.copy(
      filters = new ::(newFilters.head, newFilters.tail.toList)
    )
  }

  /**
   * Serialize the filter to the given [[OutputStream]]
   *
   * Deserializers are defined by [[ApproxFilterDeserializer]] available as an implicit
   * in the [[ApproxFilterCompanion]] object.
   */
  override def writeTo(out: OutputStream): Unit = {
    // Serial form:
    // fpProb, initialCapacity, growthRate, tighteningRatio
    // N the number of BloomFilters in this ScalableBloomFilter
    // The N BloomFilters.
    val dout = new DataOutputStream(out)
    dout.writeDouble(fpProb)
    dout.writeInt(initialCapacity)
    dout.writeInt(growthRate)
    dout.writeDouble(tighteningRatio)
    dout.writeInt(filters.size)
    filters.foreach(_.writeTo(dout))
  }
}

object ScalableBloomFilter extends ApproxFilterCompanion[ScalableBloomFilter] {
  /**
   * An implicit deserializer available when we know a Funnel instance for the
   * Filter's type.
   *
   * A deserialization doesn't require specifying any parameters like `fpProb`
   * and `numElements` and hence is available as in implicit.
   */
  implicit def deserializer[T: Funnel]: ApproxFilterDeserializer[T, ScalableBloomFilter] =
    new ApproxFilterDeserializer[T, ScalableBloomFilter] {
      override def readFrom(in: InputStream): ScalableBloomFilter[T] = {
        val din = new DataInputStream(in)

        val fpProb = din.readDouble()
        val initialCapacity = din.readInt()
        val growthRate = din.readInt()
        val tighteningRatio = din.readDouble()
        val numFilters = din.readInt()
        val filters =
          (1 to numFilters).map(_ => gBloomFilter.readFrom[T](in, implicitly[Funnel[T]])).toList

        ScalableBloomFilter[T](
          fpProb,
          initialCapacity,
          growthRate,
          tighteningRatio,
          new ::(filters.head, filters.tail) // This is a NonEmptyList
        )
      }
    }

  def par[T: Coder: Funnel](
    fpProb: Double,
    headCapacity: Int,
    growthRate: Int,
    tighteningRatio: Double
  ): ScalableBloomFilterBuilder[T] =
    ScalableBloomFilterBuilder(fpProb, headCapacity, growthRate, tighteningRatio)

  def empty[T: Funnel](
    fpProb: Double,
    initialCapacity: Int,
    growthRate: Int,
    tighteningRatio: Double
  ): ScalableBloomFilter[T] = ScalableBloomFilter(
    fpProb,
    initialCapacity,
    growthRate,
    tighteningRatio,
    new ::(gBloomFilter.create[T](implicitly[Funnel[T]], initialCapacity, fpProb), Nil)
  )
}

final case class ScalableBloomFilterBuilder[T: Funnel] private[values] (
  fpProb: Double,
  initialCapacity: Int,
  growthRate: Int,
  tighteningRatio: Double
) extends ApproxFilterBuilder[T, ScalableBloomFilter] {
  override def build(iterable: Iterable[T]): ScalableBloomFilter[T] =
    // create an empty Filter and then add all the elements to that.
    ScalableBloomFilter
      .empty(
        fpProb,
        initialCapacity,
        growthRate,
        tighteningRatio
      )
      .putAll(iterable)
}
