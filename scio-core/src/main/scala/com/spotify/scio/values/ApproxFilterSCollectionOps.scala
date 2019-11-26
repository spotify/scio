package com.spotify.scio.values
import com.google.common.hash.Funnel
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder

import scala.language.implicitConversions

class ApproxFilterSCollectionOps[T: Coder](self: SCollection[T]) {

  /**
   * Converts the SCollection to an ApproxFilter using the specified Builder.
   *
   * Generic `to`
   **/
  @experimental
  def to[C[B] <: ApproxFilter[B]](
    builder: ApproxFilterBuilder[T, C]
  )(implicit coder: Coder[C[T]]): SCollection[C[T]] =
    builder.build(self)

  // FIXME may be we don't need this. the above one is sufficient
  @experimental
  def toBloomFilter(bfBuilder: ApproxFilterBuilder[T, BloomFilter])(
    implicit coder: Coder[BloomFilter[T]]): SCollection[BloomFilter[T]] =
    bfBuilder.build(self)

  // Single threaded group all builder.
  def toBloomFilter(
    fpp: Double
  )(implicit f: Funnel[T], coder: Coder[BloomFilter[T]]): SCollection[BloomFilter[T]] =
    to(BloomFilter(fpp))

  /**
   * Goes from a `SCollection[T]` to an singleton `SCollection[BloomFilter[T]]`
   * Uses opinionated builders based on the number of elements.
   *
   * @return
   */
  def toBloomFilter(
    numElems: Int,
    fpp: Double
  )(implicit f: Funnel[T], coder: Coder[BloomFilter[T]]): SCollection[BloomFilter[T]] = {
    val settings = BloomFilter.optimalBFSettings(numElems, fpp)
    require(settings.numBFs == 1,
            s"One Bloom filter can only store up to ${settings.capacity} elements")

    if (numElems <= settings.capacity / 2) { // TODO benchmark this.
      to(BloomFilter(fpp))
    } else {
      to(BloomFilter.par(numElems, fpp))
    }
  }

  def asBloomFilterSingletonSideInput(
    fpp: Double
  )(implicit f: Funnel[T], coder: Coder[BloomFilter[T]]): SideInput[BloomFilter[T]] =
    to(BloomFilter(fpp)).asSingletonSideInput
}

trait ApproxFilterSCollectionSyntax {
  implicit def toApproxSColl[T: Coder](sc: SCollection[T]): ApproxFilterSCollectionOps[T] =
    new ApproxFilterSCollectionOps[T](sc)
}

class ApproxPairSCollectionOps[K: Coder, V: Coder](self: SCollection[(K, V)]) {
  // Single threaded group all builder.
  def toBloomFilterPerKey(
    fpp: Double
  )(implicit f: Funnel[V]): SCollection[(K, BloomFilter[V])] =
    self.groupByKey
    // TODO Coder derivation for BF wrapper is failing
      .mapValues(BloomFilter(_, fpp))(BloomFilter.coder[V], Coder[K])

  def toScalableBloomFilterPerKey(
                           fpp: Double
                         )(implicit f: Funnel[V]): SCollection[(K, BloomFilter[V])] =
    ???
}

// What the user facing API can look like.
object Example extends GuavaFunnelInstances {
  val sc = new ApproxFilterSCollectionOps[Int](null)

  val x: SCollection[BloomFilter[Int]] = sc.to(BloomFilter.par[Int](1, 0.01))

  val si: SideInput[BloomFilter[Int]] =
    sc.to(BloomFilter.par(1, 0.01)).asSingletonSideInput

  val bf: SideInput[BloomFilter[Int]] =
    sc.to(BloomFilter(0.01)).asSingletonSideInput
}
