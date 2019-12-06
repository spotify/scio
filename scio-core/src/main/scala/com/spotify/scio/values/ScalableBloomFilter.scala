package com.spotify.scio.values

import java.io.{InputStream, OutputStream}

import com.google.common.hash.{Funnel, BloomFilter => gBloomFilter}
import com.spotify.scio.coders.Coder

import scala.collection.mutable

case class ScalableBloomFilter[T] private (
  // When changing the order of the types, change the implicit Coder in companion object
  fpProb: Double,
  initialCapacity: Int,
  growthRate: Int,
  tighteningRatio: Double,
  private val filters: List[gBloomFilter[T]]
) extends ApproxFilter[T] {
  override def mayBeContains(t: T): Boolean = filters.exists(_.mightContain(t))

  /**
   * Serialize the filter to the given [[OutputStream]]
   */
  override def writeTo(out: OutputStream): Unit = ???

  def numFilters: Int = filters.size
}

object ScalableBloomFilter {
  /**
   * Use Beam Coders explicitly because of private constructor.
   * // TODO write this after complete implementation
   */
  implicit def coder[T: Funnel]: Coder[ScalableBloomFilter[T]] = Coder.kryo[ScalableBloomFilter[T]]
//    Coder.xmap(Coder[(Double, Int, Int, Double, List[gBloomFilter[T]])])(
//      ScalableBloomFilter[T].tupled,
//      ScalableBloomFilter.unapply(_).get
//    )

  def par[T: Coder: Funnel](
    fpProb: Double,
    headCapacity: Int,
    growthRate: Int,
    tighteningRatio: Double
  ) =
    ScalableBloomFilterBuilder(fpProb, headCapacity, growthRate, tighteningRatio)
}

case class ScalableBloomFilterBuilder[T: Funnel] private[values] (
  fpProb: Double,
  initialCapacity: Int,
  growthRate: Int,
  tighteningRatio: Double
) extends ApproxFilterBuilder[T, ScalableBloomFilter] {
  override def build(iterable: Iterable[T]): ScalableBloomFilter[T] = {
    val it = iterable.iterator
    val filters = mutable.ListBuffer.empty[gBloomFilter[T]]
    var numInserted = 0
    var capacity = initialCapacity
    var currentFpProb = fpProb
    while (it.hasNext && numInserted < capacity) {
      val f = gBloomFilter.create[T](implicitly[Funnel[T]], capacity, currentFpProb)
      while (it.hasNext && numInserted < capacity) {
        f.put(it.next())
        numInserted += 1
      }
      filters.insert(0, f)
      capacity *= growthRate
      currentFpProb *= tighteningRatio
    }
    ScalableBloomFilter(fpProb, initialCapacity, growthRate, tighteningRatio, filters.toList)
  }

  def readFrom(in: InputStream): ScalableBloomFilter[T] = ???
}
