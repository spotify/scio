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
      new HyperLogLogPlusPlus.Builder.normalPrecision(p).buildForIntegers()

    override def convert(t: Int): Integer = t
  }

  implicit val longHllPlus: HllPlus[Long] = new HllPlus[Long] {
    override type In = java.lang.Long

    override def hll(p: Int): HyperLogLogPlusPlus[In] =
      new HyperLogLogPlusPlus.Builder.normalPrecision(p).buildForLongs()

    override def convert(t: Long): lang.Long = t
  }

  implicit val stringHllPlus: HllPlus[String] = new HllPlus[String] {
    override type In = java.lang.String

    override def hll(p: Int): HyperLogLogPlusPlus[In] =
      new HyperLogLogPlusPlus.Builder.normalPrecision(p).buildForStrings()

    override def convert(t: String): String = t
  }

  implicit val byteStringHllPlus: HllPlus[ByteString] with Object = new HllPlus[ByteString] {
    override type In = ByteString

    override def hll(p: Int): HyperLogLogPlusPlus[In] =
      new HyperLogLogPlusPlus.Builder
        .normalPrecision(p)
        .buildForBytes()
        .asInstanceOf[HyperLogLogPlusPlus[ByteString]]

    override def convert(t: ByteString): ByteString = t
  }
}
