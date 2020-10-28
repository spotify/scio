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

import com.google.protobuf.ByteString
import com.google.zetasketch.HyperLogLogPlusPlus
import com.spotify.scio.coders.{BeamCoders, Coder}
import com.spotify.scio.estimators.ApproxDistinctCounter
import com.spotify.scio.util.TupleFunctions._
import com.spotify.scio.values.SCollection
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

  // Distributed HyperLogLogPlusPlus impl support //
  sealed trait HllPlus[T] extends Serializable {
    type IN

    def hll(): HyperLogLogPlusPlus[IN] = hll(
      HyperLogLogPlusPlus.DEFAULT_NORMAL_PRECISION
    )

    def hll(arr: Array[Byte]): HyperLogLogPlusPlus[IN] =
      HyperLogLogPlusPlus.forProto(arr).asInstanceOf[HyperLogLogPlusPlus[IN]]

    def hll(p: Int): HyperLogLogPlusPlus[IN]

    def convert(t: T): IN
  }

  object HllPlus {
    implicit val intHllPlus: HllPlus[Int] = new HllPlus[Int] {
      type IN = Integer

      override def hll(p: Int): HyperLogLogPlusPlus[Integer] =
        new HyperLogLogPlusPlus.Builder().normalPrecision(p).buildForIntegers()

      override def convert(t: Int): Integer = t
    }

    implicit val longHllPlus: HllPlus[Long] = new HllPlus[Long] {
      override type IN = java.lang.Long

      override def hll(p: Int): HyperLogLogPlusPlus[IN] =
        new HyperLogLogPlusPlus.Builder().normalPrecision(p).buildForLongs()

      override def convert(t: Long): lang.Long = t
    }

    implicit val stringHllPlus: HllPlus[String] = new HllPlus[String] {
      override type IN = java.lang.String

      override def hll(p: Int): HyperLogLogPlusPlus[IN] =
        new HyperLogLogPlusPlus.Builder().normalPrecision(p).buildForStrings()

      override def convert(t: String): String = t
    }

    implicit val byteStringHllPlus: HllPlus[ByteString] with Object = new HllPlus[ByteString] {
      override type IN = ByteString

      override def hll(p: Int): HyperLogLogPlusPlus[IN] =
        new HyperLogLogPlusPlus.Builder()
          .normalPrecision(p)
          .buildForBytes()
          .asInstanceOf[HyperLogLogPlusPlus[ByteString]]

      override def convert(t: ByteString): ByteString = t
    }
  }

  /**
   * This is a wrapper for internal HyperLogLogPlusPlus implementation.
   * @param arrOpt - serialized byte array to construct the initial HyperLogLogPlusPlus instance.
   * @param elemOpt - first element to add to the constructed new HyperLogLogPlusPlus isntance.
   */
  final class ZetaSketchHLL[T](arrOpt: Option[Array[Byte]], elemOpt: Option[T] = None)(implicit
    hp: HllPlus[T]
  ) extends Serializable {

    @transient private lazy val hll: HyperLogLogPlusPlus[hp.IN] = (arrOpt, elemOpt) match {
      case (Some(arr), Some(elem)) => {
        val x = hp.hll(arr)
        x.add(hp.convert(elem))
        x
      }
      case (Some(arr), None) => hp.hll(arr)
      case (None, Some(elem)) => {
        val x = hp.hll()
        x.add(hp.convert(elem))
        x
      }
      case (None, None) => hp.hll()
    }

    /**
     * Add a new element to this [[ZetaSketchHLL]].
     * Here we have to return a new instance to maintain the immutability of [[hll]].
     * @param elem - new element to add.
     * @return - new [[ZetaSketchHLL]] instance with the same type parameter.
     */
    def add(elem: T): ZetaSketchHLL[T] =
      new ZetaSketchHLL[T](Option(hll.serializeToByteArray()), Option(elem))

    /**
     * Merge both this and that [[ZetaSketchHLL]] instances.
     * Here we have to return a new instance to maintain the immutability of this [[hll]] and that [[hll]].
     * @param that - [[ZetaSketchHLL]] to merge with this.
     * @return new instance of [[ZetaSketchHLL]]
     */
    def merge(that: ZetaSketchHLL[T]): ZetaSketchHLL[T] = {
      val nhll = hp.hll(hll.serializeToByteArray())
      nhll.merge(that.hll.serializeToByteArray())
      new ZetaSketchHLL[T](Option(nhll.serializeToByteArray()))
    }

    /** @return the estimated distinct count */
    def estimateSize(): Long = hll.result()

    /** @return precision used in hyperloglog++ algorithm. */
    def precision: Int = hll.getNormalPrecision

    /** @return sparse precision used in hyperloglog++ algorithm. */
    def sparsePrecision: Int = hll.getSparsePrecision
  }

  object ZetaSketchHLL {
    def create[T: HllPlus](): ZetaSketchHLL[T] = new ZetaSketchHLL[T](None)

    def create[T: HllPlus](arr: Array[Byte]) = new ZetaSketchHLL[T](Option(arr))

    def create[T](p: Int)(implicit hp: HllPlus[T]): ZetaSketchHLL[T] = create(
      hp.hll(p).serializeToByteArray()
    )

    def create[T: HllPlus](elem: T): ZetaSketchHLL[T] = new ZetaSketchHLL[T](None, Option(elem))

    implicit def coder[T: HllPlus]: Coder[ZetaSketchHLL[T]] = {
      Coder.xmap[Array[Byte], ZetaSketchHLL[T]](Coder.arrayByteCoder)(
        arr => ZetaSketchHLL.create[T](arr),
        zt => zt.hll.serializeToByteArray()
      )
    }
  }

  case class ZetaSketchHLLMonoid[T: HllPlus]() extends Monoid[ZetaSketchHLL[T]] {
    override def zero: ZetaSketchHLL[T] = ZetaSketchHLL.create[T]()

    override def plus(x: ZetaSketchHLL[T], y: ZetaSketchHLL[T]): ZetaSketchHLL[T] = x.merge(y)
  }

  case class ZetaSketchHLLAggregator[T: HllPlus]()
      extends MonoidAggregator[T, ZetaSketchHLL[T], Long] {
    override def monoid: Monoid[ZetaSketchHLL[T]] = ZetaSketchHLLMonoid()

    override def prepare(input: T): ZetaSketchHLL[T] = ZetaSketchHLL.create[T](input)

    override def present(reduction: ZetaSketchHLL[T]): Long = reduction.estimateSize()
  }

  // Syntax
  implicit class ZetaSCollection[T](private val scol: SCollection[T]) extends AnyVal {

    /**
     * Convert each element to [[ZetaSketchHLL]].
     * @return [[SCollection]] of [[ZetaSketchHLL]].
     *         This will have the exactly the same number of element as input [[SCollection]]
     */
    def asZetaSketchHLL(implicit hp: HllPlus[T]): SCollection[ZetaSketchHLL[T]] =
      scol.map(ZetaSketchHLL.create[T](_))

    /**
     * Calculate the approximate distinct count using HyperLogLog++ algorithm.
     * @return - [[SCollection]] with one [[Long]] value.
     */
    def approxDistinctCountWithZetaHll(implicit hp: HllPlus[T]): SCollection[Long] =
      scol.aggregate(ZetaSketchHLLAggregator())
  }

  implicit class PairedZetaSCollection[K, V](private val kvScol: SCollection[(K, V)])
      extends AnyVal {

    /**
     * Convert each value in key-value pair to [[ZetaSketchHLL]].
     * @return key-value [[SCollection]] where value being [[ZetaSketchHLL]].
     *         This will have the similar number of elements as input [[SCollection]].
     */
    def asZetaSketchHLL(implicit hp: HllPlus[V]): SCollection[(K, ZetaSketchHLL[V])] =
      kvScol.mapValues(ZetaSketchHLL.create[V](_))

    /**
     * Calculate the approximate distinct count using HyperLogLog++ algorithm.
     * @return - [[SCollection]] with one [[Long]] value per each unique key.
     */
    def approxDistinctCountWithZetaHll(implicit hp: HllPlus[V]): SCollection[(K, Long)] =
      kvScol.aggregateByKey(ZetaSketchHLLAggregator())
  }

  implicit class ZetaSketchHLLSCollection[T](
    private val scol: SCollection[ZetaSketchHLL[T]]
  ) extends AnyVal {
    def sumZ: SCollection[ZetaSketchHLL[T]] = scol.reduce(_.merge(_))

    def approxDistinctCount: SCollection[Long] = scol.map(_.estimateSize())
  }

  implicit class ZetaSketchHLLSCollectionKV[K, V](
    private val kvSCol: SCollection[(K, ZetaSketchHLL[V])]
  ) extends AnyVal {
    def sumZ: SCollection[(K, ZetaSketchHLL[V])] = kvSCol.reduceByKey(_.merge(_))

    def approxDistinctCount: SCollection[(K, Long)] = kvSCol.mapValues(_.estimateSize())
  }
}
