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

import org.apache.beam.sdk.extensions.zetasketch.HllCount
import org.apache.beam.sdk.extensions.zetasketch.HllCount.Init

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
