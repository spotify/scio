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

import com.spotify.scio.coders.CoderMaterializer.CoderOptions
import com.spotify.scio.coders.{instances => scio}
import com.spotify.scio.values.{SCollection, SideInput}
import org.apache.beam.sdk.{coders => beam}
import org.apache.beam.sdk.values.PCollection

import scala.annotation.tailrec

/** Utility for extracting [[Coder]]s from Scio types. */
private[scio] object BeamCoders {
  @tailrec
  private def unwrap[T](options: CoderOptions, coder: beam.Coder[T]): beam.Coder[T] =
    coder match {
      case c: MaterializedCoder[T]                            => unwrap(options, c.bcoder)
      case c: beam.NullableCoder[T] if options.nullableCoders => c.getValueCoder
      case _                                                  => coder
    }

  /** Get coder from an `PCollection[T]`. */
  def getCoder[T](coll: PCollection[T]): Coder[T] = {
    val options = CoderOptions(coll.getPipeline.getOptions)
    Coder.beam(unwrap(options, coll.getCoder))
  }

  /** Get coder from an `SCollection[T]`. */
  def getCoder[T](coll: SCollection[T]): Coder[T] = getCoder(coll.internal)

  /** Get key-value coders from an `SCollection[(K, V)]`. */
  def getTupleCoders[K, V](coll: SCollection[(K, V)]): (Coder[K], Coder[V]) = {
    val options = CoderOptions(coll.context.options)
    val coder = coll.internal.getCoder
    val (k, v) = unwrap(options, coder) match {
      case c: scio.Tuple2Coder[K, V] =>
        (c.ac, c.bc)
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract key-value coders from Coder[(K, V)]: $coder"
        )
    }
    (
      Coder.beam(unwrap(options, k)),
      Coder.beam(unwrap(options, v))
    )
  }

  def getTuple3Coders[A, B, C](coll: SCollection[(A, B, C)]): (Coder[A], Coder[B], Coder[C]) = {
    val options = CoderOptions(coll.context.options)
    val coder = coll.internal.getCoder
    val (a, b, c) = unwrap(options, coder) match {
      case c: scio.Tuple3Coder[A, B, C] => (c.ac, c.bc, c.cc)
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract tupled coders from Coder[(A, B, C)]: $coder"
        )
    }
    (
      Coder.beam(unwrap(options, a)),
      Coder.beam(unwrap(options, b)),
      Coder.beam(unwrap(options, c))
    )
  }

  def getTuple4Coders[A, B, C, D](
    coll: SCollection[(A, B, C, D)]
  ): (Coder[A], Coder[B], Coder[C], Coder[D]) = {
    val options = CoderOptions(coll.context.options)
    val coder = coll.internal.getCoder
    val (a, b, c, d) = unwrap(options, coder) match {
      case c: scio.Tuple4Coder[A, B, C, D] => (c.ac, c.bc, c.cc, c.dc)
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract tupled coders from Coder[(A, B, C, D)]: $coder"
        )
    }
    (
      Coder.beam(unwrap(options, a)),
      Coder.beam(unwrap(options, b)),
      Coder.beam(unwrap(options, c)),
      Coder.beam(unwrap(options, d))
    )
  }

  private def getIterableV[V](
    options: CoderOptions,
    coder: beam.Coder[Iterable[V]]
  ): beam.Coder[V] = {
    unwrap(options, coder) match {
      case c: scio.BaseSeqLikeCoder[Iterable, V] @unchecked => c.elemCoder
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract value coder from Coder[Iterable[V]]: $coder"
        )
    }
  }

  /** Get key-value coders from a `SideInput[Map[K, Iterable[V]]]`. */
  def getMultiMapKV[K, V](si: SideInput[Map[K, Iterable[V]]]): (Coder[K], Coder[V]) = {
    val options = CoderOptions(si.view.getPCollection.getPipeline.getOptions)
    val coder = si.view.getPCollection.getCoder
    val (k, v) = unwrap(options, coder) match {
      case c: beam.KvCoder[K, V] @unchecked =>
        // Beam's `View.asMultiMap`
        (c.getKeyCoder, c.getValueCoder)
      case c: scio.MapCoder[K, Iterable[V]] @unchecked =>
        // `asMapSingletonSideInput`
        (c.kc, getIterableV(options, c.vc))
      case _ =>
        throw new IllegalArgumentException(
          s"Failed to extract key-value coders from Coder[Map[K, Iterable[V]]: $coder"
        )
    }
    (
      Coder.beam(unwrap(options, k)),
      Coder.beam(unwrap(options, v))
    )
  }
}
