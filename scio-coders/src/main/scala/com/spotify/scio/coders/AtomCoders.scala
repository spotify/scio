/*
 * Copyright 2016 Spotify AB.
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

import com.spotify.scio.coders.Coder.beam
import org.apache.beam.sdk.coders.{Coder => BCoder, _}

import scala.language.higherKinds

trait AtomCoders extends LowPriorityFallbackCoder {
  implicit def byteCoder: Coder[Byte] = beam(ByteCoder.of().asInstanceOf[BCoder[Byte]])
  implicit def stringCoder: Coder[String] = beam(StringUtf8Coder.of())
  implicit def shortCoder: Coder[Short] = beam(BigEndianShortCoder.of().asInstanceOf[BCoder[Short]])
  implicit def intCoder: Coder[Int] = beam(VarIntCoder.of().asInstanceOf[BCoder[Int]])
  implicit def longCoder: Coder[Long] = beam(BigEndianLongCoder.of().asInstanceOf[BCoder[Long]])
  implicit def floatCoder: Coder[Float] = beam(FloatCoder.of().asInstanceOf[BCoder[Float]])
  implicit def doubleCoder: Coder[Double] = beam(DoubleCoder.of().asInstanceOf[BCoder[Double]])
  implicit def booleanCoder: Coder[Boolean] = beam(BooleanCoder.of().asInstanceOf[BCoder[Boolean]])
  implicit def unitCoder: Coder[Unit] = beam(UnitCoder)
  implicit def nothingCoder: Coder[Nothing] = beam[Nothing](NothingCoder)
  implicit def bigdecimalCoder: Coder[BigDecimal] =
    Coder.xmap(beam(BigDecimalCoder.of()))(BigDecimal.apply, _.bigDecimal)

  implicit def optionCoder[T, S[_] <: Option[_]](implicit c: Coder[T]): Coder[S[T]] =
    Coder
      .transform(c) { bc =>
        Coder.beam(new OptionCoder[T](bc))
      }
      .asInstanceOf[Coder[S[T]]]

  implicit def noneCoder: Coder[None.type] =
    optionCoder[Nothing, Option](nothingCoder).asInstanceOf[Coder[None.type]]
}
