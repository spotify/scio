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

import com.google.zetasketch.HyperLogLogPlusPlus
import com.spotify.scio.coders.Coder
import com.twitter.algebird.{Monoid, MonoidAggregator}

/**
 * This is a wrapper for internal HyperLogLogPlusPlus implementation.
 *
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

  /** ZetaSketchHLL [[com.twitter.algebird.Monoid]] impl */
  final case class ZetaSketchHLLMonoid[T: HllPlus]() extends Monoid[ZetaSketchHLL[T]] {
    override def zero: ZetaSketchHLL[T] = ZetaSketchHLL.create[T]()

    override def plus(x: ZetaSketchHLL[T], y: ZetaSketchHLL[T]): ZetaSketchHLL[T] = x.merge(y)
  }

  /** ZetaSketchHLL [[com.twitter.algebird.MonoidAggregator]] impl */
  final case class ZetaSketchHLLAggregator[T: HllPlus]()
      extends MonoidAggregator[T, ZetaSketchHLL[T], Long] {
    override def monoid: Monoid[ZetaSketchHLL[T]] = ZetaSketchHLLMonoid()

    override def prepare(input: T): ZetaSketchHLL[T] = ZetaSketchHLL.create[T](input)

    override def present(reduction: ZetaSketchHLL[T]): Long = reduction.estimateSize()
  }

  /** Coder for ZetaSketchHLL */
  implicit def coder[T: HllPlus]: Coder[ZetaSketchHLL[T]] = {
    Coder.xmap[Array[Byte], ZetaSketchHLL[T]](Coder.arrayByteCoder)(
      arr => ZetaSketchHLL.create[T](arr),
      zt => zt.hll.serializeToByteArray()
    )
  }
}
