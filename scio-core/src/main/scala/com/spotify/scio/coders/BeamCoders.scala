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

package com.spotify.scio.coders

import com.spotify.scio.coders.{instances => scio}
import com.spotify.scio.values.{SCollection, SideInput}
import org.apache.beam.sdk.{coders => beam}

import scala.annotation.tailrec

/** Utility for extracting [[Coder]]s from Scio types. */
private[scio] object BeamCoders {
  @tailrec
  private def unwrap[T](coder: beam.Coder[T]): beam.Coder[T] =
    coder match {
      case WrappedBCoder(c)         => unwrap(c)
      case c: LazyCoder[T]          => unwrap(c.bcoder)
      case c: beam.NullableCoder[T] => c.getValueCoder
      case _                        => coder
    }

  /** Get key-value coders from an `SCollection[(K, V)]`. */
  def getTupleCoders[K, V](coll: SCollection[(K, V)]): (Coder[K], Coder[V]) = {
    val coder = coll.internal.getCoder
    val (k, v) = unwrap(coder) match {
      case c: scio.PairCoder[K, V] => (c.ac, c.bc)
      case c: RecordCoder[(K, V)] =>
        (
          c.cs.find(_._1 == "_1").get._2.asInstanceOf[beam.Coder[K]],
          c.cs.find(_._1 == "_2").get._2.asInstanceOf[beam.Coder[V]]
        )
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract key-value coders from Coder[(K, V)]: $coder"
        )
    }
    (Coder.beam(k), Coder.beam(v))
  }

  private def getIterableV[V](coder: beam.Coder[Iterable[V]]): beam.Coder[V] =
    unwrap(coder) match {
      case (c: scio.BaseSeqLikeCoder[Iterable, V] @unchecked) => c.elemCoder
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract value coder from Coder[Iterable[V]]: $coder"
        )
    }

  /** Get key-value coders from a `SideInput[Map[K, Iterable[V]]]`. */
  def getMultiMapKV[K, V](si: SideInput[Map[K, Iterable[V]]]): (Coder[K], Coder[V]) = {
    val coder = si.view.getPCollection.getCoder.asInstanceOf[beam.KvCoder[_, _]].getValueCoder
    val (k, v) = unwrap(coder) match {
      // Beam's `View.asMultiMap`
      case c: beam.KvCoder[K, V] => (c.getKeyCoder, c.getValueCoder)
      // `asMapSingletonSideInput`
      case (c: scio.MapCoder[K, Iterable[V]] @unchecked) => (c.kc, getIterableV(c.vc))
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract value coder from Coder[Map[K, Iterable[V]]]: $coder"
        )
    }
    (Coder.beam(k), Coder.beam(v))
  }
}
