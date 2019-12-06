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
import com.google.common.hash.Funnel
import com.spotify.scio.coders.Coder

import scala.language.implicitConversions

class ApproxFilterSCollectionOps[T: Coder](self: SCollection[T]) {
  /**
   * Converts the SCollection to an ApproxFilter using the specified Builder.
   *
   * Generic `to` for all [[ApproxFilter]] collections.
   **/
  def to[C[B] <: ApproxFilter[B]](
    builder: ApproxFilterBuilder[T, C]
  )(implicit coder: Coder[C[T]]): SCollection[C[T]] =
    builder.build(self)

  def to_[C[B] <: ApproxFilter[B]](
    builder: ApproxFilterBuilder[T, C]
  )(implicit coder: Coder[C[T]]): SCollection[C[T]] =
    builder.build(self)

  def toBloomFilter(
    fpProb: Double
  )(implicit f: Funnel[T], coder: Coder[BloomFilter[T]]): SCollection[BloomFilter[T]] =
    to(BloomFilter(fpProb))

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
    require(
      settings.numBFs == 1,
      s"One Bloom filter can only store up to ${settings.capacity} elements"
    )

    if (numElems <= settings.capacity / 2) { // TODO benchmark this.
      to(BloomFilter(fpp))
    } else {
      to(BloomFilter.par(numElems, fpp))
    }
  }

  /**
   * Create a `SideInput[BloomFilter[T]]` with the expected false positive probability to be used with
   * `SCollection[U]#withSideInputs`.
   *
   * @param fpPorb expected false positive probability
   */
  def asBloomFilterSingletonSideInput(
    fpPorb: Double
  )(implicit f: Funnel[T], coder: Coder[BloomFilter[T]]): SideInput[BloomFilter[T]] =
    to(BloomFilter(fpPorb)).asSingletonSideInput
}

trait ApproxFilterSCollectionSyntax {
  implicit def toApproxSColl[T: Coder](sc: SCollection[T]): ApproxFilterSCollectionOps[T] =
    new ApproxFilterSCollectionOps[T](sc)
}

class ApproxPairSCollectionOps[K: Coder, V: Coder](self: SCollection[(K, V)]) {
  // TODO should we have a `to` for each key like we have for global SCollection

  /**
   * Construct a [[BloomFilter]] of the values of each key in the current SCollection.
   */
  def toBloomFilterPerKey(
    fpProb: Double
  )(implicit f: Funnel[V]): SCollection[(K, BloomFilter[V])] =
    self.groupByKey
      .mapValues(BloomFilter(_, fpProb))(
        BloomFilter.coder[V], // Work around false Kryo warning
        Coder[K]
      )

  def toScalableBloomFilterPerKey(
    fpp: Double
  )(implicit f: Funnel[V]): SCollection[(K, BloomFilter[V])] =
    ???
}

// FIXME What the user facing API can look like.
object Example extends GuavaFunnelInstances {
  val sc = new ApproxFilterSCollectionOps[Int](null)

  val x: SCollection[BloomFilter[Int]] = sc.to(BloomFilter.par[Int](1, 0.01))

  val si: SideInput[BloomFilter[Int]] =
    sc.to(BloomFilter.par[Int](1, 0.01)).asSingletonSideInput

  val bf: SideInput[BloomFilter[Int]] =
    sc.to(BloomFilter(0.01)).asSingletonSideInput
}
