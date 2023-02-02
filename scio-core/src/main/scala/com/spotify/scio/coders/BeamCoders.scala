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
import org.apache.beam.sdk.values.PCollection

import scala.annotation.tailrec

/** Utility for extracting [[Coder]]s from Scio types. */
private[scio] object BeamCoders {
  @tailrec
  private def unwrap[T](coder: beam.Coder[T]): beam.Coder[T] =
    coder match {
      case c: WrappedCoder[T]       => unwrap(c.bcoder)
      case c: beam.NullableCoder[T] => c.getValueCoder
      case _                        => coder
    }

  @inline
  private def coderElement[T](productCoder: RecordCoder[_])(n: Int): beam.Coder[T] =
    productCoder.cs(n)._2.asInstanceOf[beam.Coder[T]]

  /** Get coder from an `PCollection[T]`. */
  def getCoder[T](coll: PCollection[T]): Coder[T] = Coder.beam(unwrap(coll.getCoder))

  /** Get coder from an `SCollection[T]`. */
  def getCoder[T](coll: SCollection[T]): Coder[T] = getCoder(coll.internal)

  /** Get key-value coders from an `SCollection[(K, V)]`. */
  def getTupleCoders[K, V](coll: SCollection[(K, V)]): (Coder[K], Coder[V]) = {
    val coder = coll.internal.getCoder
    val (k, v) = unwrap(coder) match {
      case c: scio.Tuple2Coder[K, V] =>
        (c.ac, c.bc)
      case c: RecordCoder[(K, V)] =>
        val ac = coderElement[K](c)(0)
        val bc = coderElement[V](c)(1)
        (ac, bc)
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract key-value coders from Coder[(K, V)]: $coder"
        )
    }
    (Coder.beam(unwrap(k)), Coder.beam(unwrap(v)))
  }

  def getTuple3Coders[A, B, C](coll: SCollection[(A, B, C)]): (Coder[A], Coder[B], Coder[C]) = {
    val coder = coll.internal.getCoder
    val (a, b, c) = unwrap(coder) match {
      case c: scio.Tuple3Coder[A, B, C] => (c.ac, c.bc, c.cc)
      case c: RecordCoder[(A, B, C)] =>
        val ac = coderElement[A](c)(0)
        val bc = coderElement[B](c)(1)
        val cc = coderElement[C](c)(2)
        (ac, bc, cc)
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract tupled coders from Coder[(A, B, C)]: $coder"
        )
    }
    (Coder.beam(unwrap(a)), Coder.beam(unwrap(b)), Coder.beam(unwrap(c)))
  }

  def getTuple4Coders[A, B, C, D](
    coll: SCollection[(A, B, C, D)]
  ): (Coder[A], Coder[B], Coder[C], Coder[D]) = {
    val coder = coll.internal.getCoder
    val (a, b, c, d) = unwrap(coder) match {
      case c: scio.Tuple4Coder[A, B, C, D] => (c.ac, c.bc, c.cc, c.dc)
      case c: RecordCoder[(A, B, C, D)] =>
        val ac = coderElement[A](c)(0)
        val bc = coderElement[B](c)(1)
        val cc = coderElement[C](c)(2)
        val dc = coderElement[D](c)(3)
        (ac, bc, cc, dc)
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract tupled coders from Coder[(A, B, C, D)]: $coder"
        )
    }
    (Coder.beam(unwrap(a)), Coder.beam(unwrap(b)), Coder.beam(unwrap(c)), Coder.beam(unwrap(d)))
  }

  private def getIterableV[V](coder: beam.Coder[Iterable[V]]): beam.Coder[V] =
    unwrap(coder) match {
      case c: scio.BaseSeqLikeCoder[Iterable, V] @unchecked => c.elemCoder
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract value coder from Coder[Iterable[V]]: $coder"
        )
    }

  /** Get key-value coders from a `SideInput[Map[K, Iterable[V]]]`. */
  def getMultiMapKV[K, V](si: SideInput[Map[K, Iterable[V]]]): (Coder[K], Coder[V]) = {
    val coder = si.view.getPCollection.getCoder
    val (k, v) = unwrap(coder) match {
      // Beam's `View.asMultiMap`
      case c: beam.KvCoder[K, V] @unchecked => (c.getKeyCoder, c.getValueCoder)
      // `asMapSingletonSideInput`
      case c: scio.MapCoder[K, Iterable[V]] @unchecked => (c.kc, getIterableV(c.vc))
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract key-value coders from Coder[Map[K, Iterable[V]]: $coder"
        )
    }
    (Coder.beam(unwrap(k)), Coder.beam(unwrap(v)))
  }
}
