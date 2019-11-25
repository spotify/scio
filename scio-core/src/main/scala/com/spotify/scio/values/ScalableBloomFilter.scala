package com.spotify.scio.values

import com.google.common.hash.{Funnel, BloomFilter => gBloomFilter}
import com.spotify.scio.coders.Coder

case class ScalableBloomFilter[T] private (
  // When changing the order of the types, change the implicit Coder in companion object
  fpProb: Double,
  headCapacity: Int,
  growthRate: Int,
  tighteningRatio: Double,
  private val filters: List[gBloomFilter[T]]
) extends ApproxFilter[T] {

  override def mayBeContains(t: T): Boolean = filters.exists(_.mightContain(t))
  override def serialize: Array[Byte] = ???
}

object ScalableBloomFilter {

  /**
   * Use Beam Coders explicitly because of private constructor.
   */
//  implicit def coder[T: Funnel]: Coder[ScalableBloomFilter[T]] = Coder.kryo[ScalableBloomFilter[T]]
//    Coder.xmap(Coder[(Double, Int, Int, Double, List[gBloomFilter[T]])])(
//      ScalableBloomFilter[T].tupled,
//      ScalableBloomFilter.unapply(_).get
//    )

  def par[T: Coder: Funnel](fpProb: Double,
                            headCapacity: Int,
                            growthRate: Int,
                            tighteningRatio: Double) =
    ScalableBloomFilterBuilder(fpProb, headCapacity, growthRate, tighteningRatio)

}

case class ScalableBloomFilterBuilder[T: Coder: Funnel] private[values] (
  fpProb: Double,
  headCapacity: Int,
  growthRate: Int,
  tighteningRatio: Double
) extends ApproxFilterBuilder[T, ScalableBloomFilter] {

  override def build(sc: SCollection[T]): SCollection[ScalableBloomFilter[T]] = {

    ???
  }

  override def fromBytes(serializedBytes: Array[Byte]): ScalableBloomFilter[T] = ???
}
