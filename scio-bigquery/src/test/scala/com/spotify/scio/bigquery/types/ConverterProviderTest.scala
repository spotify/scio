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

package com.spotify.scio.bigquery.types

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import com.google.protobuf.ByteString
import org.scalacheck.Prop.{all, forAll}
import org.scalacheck._
import org.scalacheck.Shapeless._

object ConverterProviderTest extends Properties("ConverterProvider") {

  import Schemas._

  implicit val arbInstant = Arbitrary(Gen.const(Instant.now()))
  implicit val arbByteArray = Arbitrary(Gen.alphaStr.map(_.getBytes))
  implicit val arbByteString = Arbitrary(Gen.alphaStr.map(ByteString.copyFromUtf8))
  implicit val arbDate = Arbitrary(Gen.const(LocalDate.now()))
  implicit val arbTime = Arbitrary(Gen.const(LocalTime.now()))
  implicit val arbDatetime = Arbitrary(Gen.const(LocalDateTime.now()))

  val toBS = (b: Array[Byte]) => ByteString.copyFrom(b)

  property("round trip required primitive types") = forAll { r1: Required =>
    val r2 = BigQueryType.fromTableRow[Required](BigQueryType.toTableRow[Required](r1))
    all(
      r1.copy(byteArrayF = null) == r2.copy(byteArrayF = null),
      toBS(r1.byteArrayF) == toBS(r2.byteArrayF)
    )
  }

  property("round trip optional primitive types") = forAll { r1: Optional =>
    val r2 = BigQueryType.fromTableRow[Optional](BigQueryType.toTableRow[Optional](r1))
    all(
      r1.copy(byteArrayF = None) == r2.copy(byteArrayF = None),
      r1.byteArrayF.map(toBS) == r2.byteArrayF.map(toBS)
    )
  }

  property("round trip repeated primitive types") = forAll { r1: Repeated =>
    val r2 = BigQueryType.fromTableRow[Repeated](BigQueryType.toTableRow[Repeated](r1))
    all(
      r1.copy(byteArrayF = Nil) == r2.copy(byteArrayF = Nil),
      r1.byteArrayF.map(toBS) == r2.byteArrayF.map(toBS)
    )
  }

  property("round trip required nested types") = forAll { r1: RequiredNested =>
    val r2 = BigQueryType.fromTableRow[RequiredNested](BigQueryType.toTableRow[RequiredNested](r1))
    val r1a = r1.copy(
      r1.required.copy(byteArrayF = null),
      r1.optional.copy(byteArrayF = None),
      r1.repeated.copy(byteArrayF = Nil))
    val r2a = r2.copy(
      r2.required.copy(byteArrayF = null),
      r2.optional.copy(byteArrayF = None),
      r2.repeated.copy(byteArrayF = Nil))
    all(
      r1a == r2a,
      toBS(r1.required.byteArrayF) == toBS(r2.required.byteArrayF),
      r1.optional.byteArrayF.map(toBS) == r2.optional.byteArrayF.map(toBS),
      r1.repeated.byteArrayF.map(toBS) == r2.repeated.byteArrayF.map(toBS)
    )
  }

  property("round trip optional nested types") = forAll { r1: OptionalNested =>
    val r2 = BigQueryType.fromTableRow[OptionalNested](BigQueryType.toTableRow[OptionalNested](r1))
    val r1a = r1.copy(
      r1.required.map(_.copy(byteArrayF = null)),
      r1.optional.map(_.copy(byteArrayF = None)),
      r1.repeated.map(_.copy(byteArrayF = Nil)))
    val r2a = r2.copy(
      r2.required.map(_.copy(byteArrayF = null)),
      r2.optional.map(_.copy(byteArrayF = None)),
      r2.repeated.map(_.copy(byteArrayF = Nil)))
    all(
      r1a == r2a,
      r1.required.map(_.byteArrayF).map(toBS) == r2.required.map(_.byteArrayF).map(toBS),
      r1.optional.flatMap(_.byteArrayF).map(toBS) == r2.optional.flatMap(_.byteArrayF).map(toBS),
      r1.repeated.toList.flatMap(_.byteArrayF).map(toBS) ==
        r2.repeated.toList.flatMap(_.byteArrayF).map(toBS)
    )
  }

}
