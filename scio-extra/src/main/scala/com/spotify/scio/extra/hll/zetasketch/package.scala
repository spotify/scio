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

package com.spotify.scio.extra.hll

import java.lang

import com.google.zetasketch.HyperLogLogPlusPlus
import com.spotify.scio.coders.{BeamCoders, Coder}
import com.spotify.scio.estimators.ApproxDistinctCounter
import com.spotify.scio.util.TupleFunctions._
import com.spotify.scio.values.SCollection
import com.twitter.algebird
import com.twitter.algebird.{Monoid, MonoidAggregator}
import org.apache.beam.sdk.extensions.zetasketch.HllCount
import org.apache.beam.sdk.extensions.zetasketch.HllCount.Init
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{KV, PCollection}

package object zetasketch {

  sealed trait ZetaSketchable[T] {
    type IN
    def init(p: Int): Init.Builder[IN]
  }

  object ZetaSketchable {

    implicit val intZetaSketchable: ZetaSketchable[Int] = new ZetaSketchable[Int] {
      type IN = lang.Integer
      override def init(p: Int): Init.Builder[lang.Integer] =
        HllCount.Init.forIntegers().withPrecision(p)
    }

    implicit val longZetaSketchable: ZetaSketchable[Long] = new ZetaSketchable[Long] {
      type IN = lang.Long

      override def init(p: Int): Init.Builder[lang.Long] = HllCount.Init.forLongs().withPrecision(p)
    }

    implicit val stringZetaSketchable: ZetaSketchable[String] = new ZetaSketchable[String] {
      override type IN = String

      override def init(p: Int): Init.Builder[String] =
        HllCount.Init.forStrings().withPrecision(p)
    }

    implicit val byteArrayZetaSketchable: ZetaSketchable[Array[Byte]] =
      new ZetaSketchable[Array[Byte]] {
        override type IN = Array[Byte]

        override def init(p: Int): Init.Builder[Array[Byte]] =
          HllCount.Init.forBytes().withPrecision(p)
      }
  }

  /**
   * [[com.spotify.scio.estimators.ApproxDistinctCounter]] implementation for
   * [[org.apache.beam.sdk.extensions.zetasketch.HllCount]].
   * HllCount estimate the distinct count using HyperLogLogPlusPlus (HLL++) sketches on data streams based on
   * the ZetaSketch implementation.
   *
   * The HyperLogLog++ (HLL++) algorithm estimates the number of distinct values in a data stream.
   * HLL++ is based on HyperLogLog; HLL++ more accurately estimates the number of distinct values in very large and
   * small data streams.
   *
   * @param p Precision, controls the accuracy of the estimation. The precision value will have an impact on the number of buckets
   *          used to store information about the distinct elements.
   *          should be in the range `[10, 24]`, default precision value is `15`.
   */
  case class ZetaSketchHllPlusPlus[T](p: Int = HllCount.DEFAULT_PRECISION)(implicit
    zs: ZetaSketchable[T]
  ) extends ApproxDistinctCounter[T] {

    require(p >= 10 && p <= 24, "Precision(p) should be in the ragne [10, 24]")

    /**
     * Return a SCollection with single (Long)value which is the estimated distinct count in the given SCollection with
     * type `T`
     */
    override def estimateDistinctCount(in: SCollection[T]): SCollection[Long] =
      in.applyTransform(
        zs.init(p).globally().asInstanceOf[PTransform[PCollection[T], PCollection[Array[Byte]]]]
      ).applyTransform(HllCount.Extract.globally())
        .asInstanceOf[SCollection[Long]]

    /**
     * Approximate distinct element per each key in the given key value SCollection.
     * This will output estimated distinct count per each unique key.
     */
    override def estimateDistinctCountPerKey[K](in: SCollection[(K, T)]): SCollection[(K, Long)] = {
      implicit val (keyCoder, _): (Coder[K], Coder[T]) =
        BeamCoders.getTupleCoders(in)
      in.toKV
        .applyTransform(
          zs.init(p)
            .perKey[K]()
            .asInstanceOf[PTransform[PCollection[KV[K, T]], PCollection[KV[K, Array[Byte]]]]]
        )
        .applyTransform(HllCount.Extract.perKey[K]())
        .map(klToTuple)
    }
  }

  sealed trait ZetaSketchHLL[T] {
    type IN

    def hll(arr: Array[Byte]): HyperLogLogPlusPlus[IN]

    def hll(p: Int): HyperLogLogPlusPlus[IN]
  }

  object ZetaSketchHLL {

    implicit val intZetaSketchHLL = new ZetaSketchHLL[Int] {
      override type IN = lang.Integer

      override def hll(p: Int): HyperLogLogPlusPlus[lang.Integer] =
        new HyperLogLogPlusPlus.Builder().normalPrecision(p).buildForIntegers()

      override def hll(arr: Array[Byte]): HyperLogLogPlusPlus[lang.Integer] =
        HyperLogLogPlusPlus.forProto(arr).asInstanceOf[HyperLogLogPlusPlus[lang.Integer]]
    }

  }

  final class ZetaHLLPlus[T](protected val arr: Array[Byte])(implicit
    zt: ZetaSketchHLL[T]
  ) {
    private[zetasketch] val hll = zt.hll(arr)

    def add(elem: T): ZetaHLLPlus[T] = {
      hll.add(elem.asInstanceOf[zt.IN])
      this
    }

    def merge(that: ZetaHLLPlus[T]): ZetaHLLPlus[T] = {
      hll.merge(that.hll.asInstanceOf[HyperLogLogPlusPlus[zt.IN]])
      this
    }

    def estimateSize: Long = hll.result()

    def precision: Int = hll.getNormalPrecision

    def sparsePrecision: Int = hll.getSparsePrecision
  }

  object ZetaHLLPlus {

    def create[T: ZetaSketchHLL](p: Int) = new ZetaHLLPlus(
      implicitly[ZetaSketchHLL[T]].hll(p).serializeToByteArray()
    )
  }

  implicit def coder[T: ZetaSketchHLL]: Coder[ZetaHLLPlus[T]] = {
    Coder.xmap[Array[Byte], ZetaHLLPlus[T]](Coder.arrayByteCoder)(
      arr => new ZetaHLLPlus[T](arr),
      zt => zt.hll.serializeToByteArray()
    )
  }

// case class ZetaSketchHLLMonoid[T: ZetaSketchHLL]() extends Monoid[ZetaSketchHLL[T]] {
//    override def zero: ZetaSketchHLL[T] = ZetaSketchHLL.create[T]()
//
//    override def plus(x: ZetaSketchHLL[T], y: ZetaSketchHLL[T]): ZetaSketchHLL[T] = x.merge(y)
//  }
//
//  class ZetaSketchHLLAggregator[T: ZetaSketchHLLMonoid: ZetaSketchHLL]()
//      extends MonoidAggregator[T, ZetaSketchHLL[T], Long] {
//    override def monoid: Monoid[ZetaSketchHLL[T]] = implicitly[ZetaSketchHLLMonoid[T]]
//
//    override def prepare(input: T): ZetaSketchHLL[T] = ZetaSketchHLL.create(input)
//
//    override def present(reduction: ZetaSketchHLL[T]): Long = reduction.estimateCount()
//  }

  // Syntax
  implicit class ZetaSCollection[T](private val scol: SCollection[T]) extends AnyVal {
    def asZetaSketchHLL(implicit zt: ZetaSketchHLL[T]): SCollection[ZetaHLLPlus[T]] =
      scol.map(ZetaHLLPlus.create[T](15).add(_))
  }

  implicit class ZetaSketchHLLSCollection[T](
    private val scol: SCollection[ZetaHLLPlus[T]]
  ) extends AnyVal {
    def sumZ(): SCollection[ZetaHLLPlus[T]] = scol.reduce(_.merge(_))

    def estimateSize(): SCollection[Long] = scol.map(_.estimateSize)
  }
}
