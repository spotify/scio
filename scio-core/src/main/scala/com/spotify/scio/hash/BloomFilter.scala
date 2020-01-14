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

package com.spotify.scio.hash

import java.io.{InputStream, OutputStream}

import com.google.common.hash.{Funnel, BloomFilter => GBF}
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.AtomicCoder

sealed trait ApproxFilter[T] extends Serializable {
  def mightContain(elem: T): Boolean
}

class BloomFilter[T: Funnel] private (private val impl: GBF[T]) extends ApproxFilter[T] {
  override def mightContain(elem: T): Boolean = impl.mightContain(elem)
}

object BloomFilter {
  private class BloomFilterCoder[T](private implicit val fnl: Funnel[T])
    extends AtomicCoder[BloomFilter[T]] {
    override def encode(value: BloomFilter[T], outStream: OutputStream): Unit =
      value.impl.writeTo(outStream)
    override def decode(inStream: InputStream): BloomFilter[T] =
      new BloomFilter[T](GBF.readFrom(inStream, fnl))
  }

  implicit def bfCoder[T: Funnel]: Coder[BloomFilter[T]] = Coder.beam(new BloomFilterCoder[T]())

  def create[T](elems: Iterable[T])(implicit fnl: Funnel[T]): BloomFilter[T] =
    create(elems, elems.size)

  def create[T](elems: Iterable[T], expectedInsertions: Long)(implicit fnl: Funnel[T]): BloomFilter[T] =
    create(elems, expectedInsertions, 0.03)

  def create[T](elems: Iterable[T], expectedInsertions: Long, fpp: Double)(implicit fnl: Funnel[T]): BloomFilter[T] = {
    val impl = GBF.create(fnl, expectedInsertions, fpp)
    elems.foreach(impl.put)
    new BloomFilter[T](impl)
  }
}

class SetFilter[T] private (private val impl: Set[T]) extends ApproxFilter[T] {
  override def mightContain(elem: T): Boolean = impl.contains(elem)
}

object SetFilter {
  implicit def sfCoder[T](implicit elemCoder: Coder[T]): Coder[SetFilter[T]] =
    Coder.xmap(Coder[Set[T]])(new SetFilter[T](_), _.impl)

  def create[T](elems: Iterable[T]): SetFilter[T] = new SetFilter[T](elems.toSet)
}
