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

package com.spotify.scio.extra.hll.zetasketch

import java.lang

import com.google.protobuf.ByteString
import com.google.zetasketch.HyperLogLogPlusPlus
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.twitter.algebird.{Monoid, MonoidAggregator}

package object distributed {

  // Distributed HyperLogLogPlusPlus impl support //
  sealed trait HllPlus[T] extends Serializable {
    type In

    def hll(): HyperLogLogPlusPlus[In] = hll(
      HyperLogLogPlusPlus.DEFAULT_NORMAL_PRECISION
    )

    def hll(arr: Array[Byte]): HyperLogLogPlusPlus[In] =
      HyperLogLogPlusPlus.forProto(arr).asInstanceOf[HyperLogLogPlusPlus[In]]

    def hll(p: Int): HyperLogLogPlusPlus[In]

    def convert(t: T): In
  }

  object HllPlus {
    implicit val intHllPlus: HllPlus[Int] = new HllPlus[Int] {
      type In = Integer

      override def hll(p: Int): HyperLogLogPlusPlus[In] =
        new HyperLogLogPlusPlus.Builder().normalPrecision(p).buildForIntegers()

      override def convert(t: Int): Integer = t
    }

    implicit val longHllPlus: HllPlus[Long] = new HllPlus[Long] {
      override type In = java.lang.Long

      override def hll(p: Int): HyperLogLogPlusPlus[In] =
        new HyperLogLogPlusPlus.Builder().normalPrecision(p).buildForLongs()

      override def convert(t: Long): lang.Long = t
    }

    implicit val stringHllPlus: HllPlus[String] = new HllPlus[String] {
      override type In = java.lang.String

      override def hll(p: Int): HyperLogLogPlusPlus[In] =
        new HyperLogLogPlusPlus.Builder().normalPrecision(p).buildForStrings()

      override def convert(t: String): String = t
    }

    implicit val byteStringHllPlus: HllPlus[ByteString] with Object = new HllPlus[ByteString] {
      override type In = ByteString

      override def hll(p: Int): HyperLogLogPlusPlus[In] =
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
   * @param elemOpt - first element to add to the constructed new HyperLogLogPlusPlus instance.
   */
  final class ZetaSketchHLL[T](arrOpt: Option[Array[Byte]], elemOpt: Option[T] = None)(implicit
    hp: HllPlus[T]
  ) extends Serializable {

    @transient private lazy val hll: HyperLogLogPlusPlus[hp.In] = (arrOpt, elemOpt) match {
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
      case _ => hp.hll()
    }

    /**
     * Add a new element to this [[ZetaSketchHLL]].
     * Need to return a new instance to maintain the immutability of the [[hll]] value.
     * @param elem - new element to add.
     * @return - new [[ZetaSketchHLL]] instance with the same type parameter.
     */
    def add(elem: T): ZetaSketchHLL[T] =
      new ZetaSketchHLL[T](Option(hll.serializeToByteArray()), Option(elem))

    /**
     * Merge both this and that [[ZetaSketchHLL]] instances.
     * Need to return a new instance to maintain the immutability of this [[hll]] value and that [[hll]] value.
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
     * Only support for Int, Long, String and ByteString types.
     *
     * @Example
     * {{{
     *   val input: SCollection[T] = ...
     *   val zCol: SCollection[ZetaSketchHll[T]] = input.asZetaSketchHll
     *   val approxDistCount: SCollection[Long] = zCol.sumHll.approxDistinctCount
     * }}}
     *
     * [[ZetaSketchHLL]] has few extra methods to access precision, sparse precision.
     *
     * @return [[SCollection]] of [[ZetaSketchHLL]].
     *         This will have the exactly the same number of element as input [[SCollection]]
     */
    def asZetaSketchHLL(implicit hp: HllPlus[T]): SCollection[ZetaSketchHLL[T]] =
      scol.map(ZetaSketchHLL.create[T](_))

    /**
     * Calculate the approximate distinct count using HyperLogLog++ algorithm.
     * Only support for Int, Long, String and ByteString types.
     *
     * @Example
     * {{{
     *   val input: SCollection[T] = ...
     *   val approxDistCount: SCollection[Long] = input.approxDistinctCountWithZetaHll
     * }}}
     *
     * @return - [[SCollection]] with one [[Long]] value.
     */
    def approxDistinctCountWithZetaHll(implicit hp: HllPlus[T]): SCollection[Long] =
      scol.aggregate(ZetaSketchHLLAggregator())
  }

  implicit class PairedZetaSCollection[K, V](private val kvScol: SCollection[(K, V)])
      extends AnyVal {

    /**
     * Convert each value in key-value pair to [[ZetaSketchHLL]].
     * Only support for Int, Long, String and ByteString value types.
     *
     * @Example
     * {{{
     *   val input: SCollection[(K, V)] = ...
     *   val zCol: SCollection[(K, ZetaSketchHLL[V])] = input.asZetaSketchHllByKey
     *   val approxDistCount: SCollection[(K, Long)] = zCol.sumHllByKey.approxDistinctCountByKey
     * }}}
     *
     * [[ZetaSketchHLL]] has few extra methods to access precision, sparse precision.
     *
     * @return key-value [[SCollection]] where value being [[ZetaSketchHLL]].
     *         This will have the similar number of elements as input [[SCollection]].
     */
    def asZetaSketchHLLByKey(implicit hp: HllPlus[V]): SCollection[(K, ZetaSketchHLL[V])] =
      kvScol.mapValues(ZetaSketchHLL.create[V](_))

    /**
     * Calculate the approximate distinct count using HyperLogLog++ algorithm.
     * Only support for Int, Long, String and ByteString value types.
     *
     * @Example
     * {{{
     *   val input: SCollection[(K, V)] = ...
     *   val approxDistCount: SCollection[(K, Long)] = input.approxDistinctCountWithZetaHllByKey
     * }}}
     *
     * @return - [[SCollection]] with one [[Long]] value per each unique key.
     */
    def approxDistinctCountWithZetaHllByKey(implicit hp: HllPlus[V]): SCollection[(K, Long)] =
      kvScol.aggregateByKey(ZetaSketchHLLAggregator())
  }

  implicit class ZetaSketchHLLSCollection[T](
    private val scol: SCollection[ZetaSketchHLL[T]]
  ) extends AnyVal {
    def sumHll: SCollection[ZetaSketchHLL[T]] = scol.reduce(_.merge(_))

    def approxDistinctCount: SCollection[Long] = scol.map(_.estimateSize())
  }

  implicit class ZetaSketchHLLSCollectionKV[K, V](
    private val kvSCol: SCollection[(K, ZetaSketchHLL[V])]
  ) extends AnyVal {
    def sumHllByKey: SCollection[(K, ZetaSketchHLL[V])] = kvSCol.reduceByKey(_.merge(_))

    def approxDistinctCountByKey: SCollection[(K, Long)] = kvSCol.mapValues(_.estimateSize())
  }
}
