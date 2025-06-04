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
 * @param arrOpt
 *   - serialized byte array to construct the initial HyperLogLogPlusPlus instance.
 * @param elemOpt
 *   - first element to add to the constructed new HyperLogLogPlusPlus instance.
 */
final class ZetaSketchHll[T](arrOpt: Option[Array[Byte]], elemOpt: Option[T] = None)(implicit
  hp: HllPlus[T]
) extends Serializable {

  @transient private lazy val hll: HyperLogLogPlusPlus[hp.In] = (arrOpt, elemOpt) match {
    case (Some(arr), Some(elem)) => {
      val x = hp.hll(arr)
      x.add(hp.convert(elem))
      x
    }
    case (Some(arr), None)  => hp.hll(arr)
    case (None, Some(elem)) => {
      val x = hp.hll()
      x.add(hp.convert(elem))
      x
    }
    case _ => hp.hll()
  }

  /**
   * Add a new element to this [[ZetaSketchHll]]. Need to return a new instance to maintain the
   * immutability of the [[hll]] value.
   * @param elem
   *   - new element to add.
   * @return
   *   - new [[ZetaSketchHll]] instance with the same type parameter.
   */
  def add(elem: T): ZetaSketchHll[T] =
    new ZetaSketchHll[T](Option(hll.serializeToByteArray()), Option(elem))

  /**
   * Merge both this and that [[ZetaSketchHll]] instances. Need to return a new instance to maintain
   * the immutability of this [[hll]] value and that [[hll]] value.
   * @param that
   *   - [[ZetaSketchHll]] to merge with this.
   * @return
   *   new instance of [[ZetaSketchHll]]
   */
  def merge(that: ZetaSketchHll[T]): ZetaSketchHll[T] = {
    val nhll = hp.hll(hll.serializeToByteArray())
    nhll.merge(that.hll.serializeToByteArray())
    new ZetaSketchHll[T](Option(nhll.serializeToByteArray()))
  }

  /** @return the byte array representation of the hll */
  def serializeToByteArray(): Array[Byte] = hll.serializeToByteArray()

  /** @return the estimated distinct count */
  def estimateSize(): Long = hll.result()

  /** @return precision used in hyperloglog++ algorithm. */
  def precision: Int = hll.getNormalPrecision

  /** @return sparse precision used in hyperloglog++ algorithm. */
  def sparsePrecision: Int = hll.getSparsePrecision
}

object ZetaSketchHll {
  def create[T: HllPlus](): ZetaSketchHll[T] = new ZetaSketchHll[T](None)

  def create[T: HllPlus](arr: Array[Byte]) = new ZetaSketchHll[T](Option(arr))

  def create[T](p: Int)(implicit hp: HllPlus[T]): ZetaSketchHll[T] = create(
    hp.hll(p).serializeToByteArray()
  )

  def create[T: HllPlus](elem: T): ZetaSketchHll[T] = new ZetaSketchHll[T](None, Option(elem))

  /** ZetaSketchHll [[com.twitter.algebird.Monoid]] impl */
  final case class ZetaSketchHllMonoid[T: HllPlus]() extends Monoid[ZetaSketchHll[T]] {
    override def zero: ZetaSketchHll[T] = ZetaSketchHll.create[T]()

    override def plus(x: ZetaSketchHll[T], y: ZetaSketchHll[T]): ZetaSketchHll[T] = x.merge(y)
  }

  /** ZetaSketchHll [[com.twitter.algebird.MonoidAggregator]] impl */
  final case class ZetaSketchHllAggregator[T: HllPlus]()
      extends MonoidAggregator[T, ZetaSketchHll[T], Long] {
    override def monoid: Monoid[ZetaSketchHll[T]] = ZetaSketchHllMonoid()

    override def prepare(input: T): ZetaSketchHll[T] = ZetaSketchHll.create[T](input)

    override def present(reduction: ZetaSketchHll[T]): Long = reduction.estimateSize()
  }

  /** Coder for ZetaSketchHll */
  implicit def coder[T: HllPlus]: Coder[ZetaSketchHll[T]] = {
    Coder.xmap[Array[Byte], ZetaSketchHll[T]](Coder.arrayByteCoder)(
      arr => ZetaSketchHll.create[T](arr),
      zt => zt.hll.serializeToByteArray()
    )
  }
}
