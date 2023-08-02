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
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.coders.{Coder => BCoder, NullableCoder}
import org.apache.beam.sdk.values.PCollection

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

/** Utility for extracting [[Coder]]s from Scio types. */
private[scio] object BeamCoders {
  @tailrec
  private def unwrap[T](options: CoderOptions, coder: BCoder[T]): BCoder[T] =
    coder match {
      case c: MaterializedCoder[T]                       => unwrap(options, c.bcoder)
      case c: NullableCoder[T] if options.nullableCoders => c.getValueCoder
      case _                                             => coder
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
    Some(unwrap(options, coder))
      .map(_.getCoderArguments.asScala.toList)
      .collect { case (c1: BCoder[K]) :: (c2: BCoder[V]) :: Nil =>
        val k = Coder.beam(unwrap(options, c1))
        val v = Coder.beam(unwrap(options, c2))
        k -> v
      }
      .getOrElse {
        throw new IllegalArgumentException(
          s"Failed to extract key-value coders from Coder[(K, V)]: $coder"
        )
      }
  }

  def getTuple3Coders[A, B, C](coll: SCollection[(A, B, C)]): (Coder[A], Coder[B], Coder[C]) = {
    val options = CoderOptions(coll.context.options)
    val coder = coll.internal.getCoder
    Some(unwrap(options, coder))
      .map(_.getCoderArguments.asScala.toList)
      .collect { case (c1: BCoder[A]) :: (c2: BCoder[B]) :: (c3: BCoder[C]) :: Nil =>
        val a = Coder.beam(unwrap(options, c1))
        val b = Coder.beam(unwrap(options, c2))
        val c = Coder.beam(unwrap(options, c3))
        (a, b, c)
      }
      .getOrElse {
        throw new IllegalArgumentException(
          s"Failed to extract tupled coders from Coder[(A, B, C)]: $coder"
        )
      }
  }

  def getTuple4Coders[A, B, C, D](
    coll: SCollection[(A, B, C, D)]
  ): (Coder[A], Coder[B], Coder[C], Coder[D]) = {
    val options = CoderOptions(coll.context.options)
    val coder = coll.internal.getCoder
    Some(unwrap(options, coder))
      .map(_.getCoderArguments.asScala.toList)
      .collect {
        case (c1: BCoder[A]) :: (c2: BCoder[B]) :: (c3: BCoder[C]) :: (c4: BCoder[D]) :: Nil =>
          val a = Coder.beam(unwrap(options, c1))
          val b = Coder.beam(unwrap(options, c2))
          val c = Coder.beam(unwrap(options, c3))
          val d = Coder.beam(unwrap(options, c4))
          (a, b, c, d)
      }
      .getOrElse {
        throw new IllegalArgumentException(
          s"Failed to extract tupled coders from Coder[(A, B, C, D)]: $coder"
        )
      }
  }
}
