/*
 * Copyright 2018 Spotify AB.
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

import com.google.protobuf.ByteString
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck._
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import shapeless.datatype.record._

class ConverterProviderSpec extends PropSpec with GeneratorDrivenPropertyChecks with Matchers {

  // TODO: remove this once https://github.com/scalatest/scalatest/issues/1090 is addressed
  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  import Schemas._

  implicit val arbByteArray = Arbitrary(Gen.alphaStr.map(_.getBytes))
  implicit val arbByteString = Arbitrary(Gen.alphaStr.map(ByteString.copyFromUtf8))
  implicit val arbInstant = Arbitrary(Gen.const(Instant.now()))
  implicit val arbDate = Arbitrary(Gen.const(LocalDate.now()))
  implicit val arbTime = Arbitrary(Gen.const(LocalTime.now()))
  implicit val arbDatetime = Arbitrary(Gen.const(LocalDateTime.now()))

  implicit def compareByteArrays(x: Array[Byte], y: Array[Byte]): Boolean =
    ByteString.copyFrom(x) == ByteString.copyFrom(y)

  property("round trip required primitive types") {
    forAll { r1: Required =>
      val r2 = BigQueryType.fromTableRow[Required](BigQueryType.toTableRow[Required](r1))
      RecordMatcher[Required](r1, r2) shouldBe true
    }
  }

  property("round trip optional primitive types") {
    forAll { r1: Optional =>
      val r2 = BigQueryType.fromTableRow[Optional](BigQueryType.toTableRow[Optional](r1))
      RecordMatcher[Optional](r1, r2) shouldBe true
    }
  }

  property("skip null optional primitive types") {
    forAll { o: Optional =>
      val r = BigQueryType.toTableRow[Optional](o)
      // TableRow object should only contain a key if the corresponding Option[T] is defined
      o.boolF.isDefined shouldBe r.containsKey("boolF")
      o.intF.isDefined shouldBe r.containsKey("intF")
      o.longF.isDefined shouldBe r.containsKey("longF")
      o.floatF.isDefined shouldBe r.containsKey("floatF")
      o.doubleF.isDefined shouldBe r.containsKey("doubleF")
      o.stringF.isDefined shouldBe r.containsKey("stringF")
      o.byteArrayF.isDefined shouldBe r.containsKey("byteArrayF")
      o.byteStringF.isDefined shouldBe r.containsKey("byteStringF")
      o.timestampF.isDefined shouldBe r.containsKey("timestampF")
      o.dateF.isDefined shouldBe r.containsKey("dateF")
      o.timeF.isDefined shouldBe r.containsKey("timeF")
      o.datetimeF.isDefined shouldBe r.containsKey("datetimeF")
    }
  }

  property("round trip repeated primitive types") {
    forAll { r1: Repeated =>
      val r2 = BigQueryType.fromTableRow[Repeated](BigQueryType.toTableRow[Repeated](r1))
      RecordMatcher[Repeated](r1, r2) shouldBe true
    }
  }

  property("round trip required nested types") {
    forAll { r1: RequiredNested =>
      val r2 = BigQueryType.fromTableRow[RequiredNested](
        BigQueryType.toTableRow[RequiredNested](r1))
      RecordMatcher[RequiredNested](r1, r2) shouldBe true
    }
  }

  property("round trip optional nested types") {
    forAll { r1: OptionalNested =>
      val r2 = BigQueryType.fromTableRow[OptionalNested](
        BigQueryType.toTableRow[OptionalNested](r1))
      RecordMatcher[OptionalNested](r1, r2) shouldBe true
    }
  }

  property("skip null optional nested types") {
    forAll { o: OptionalNested =>
      val r = BigQueryType.toTableRow[OptionalNested](o)
      // TableRow object should only contain a key if the corresponding Option[T] is defined
      o.required.isDefined shouldBe r.containsKey("required")
      o.optional.isDefined shouldBe r.containsKey("optional")
      o.repeated.isDefined shouldBe r.containsKey("repeated")
    }
  }

  property("round trip repeated nested types") {
    forAll { r1: RepeatedNested =>
      val r2 = BigQueryType.fromTableRow[RepeatedNested](
        BigQueryType.toTableRow[RepeatedNested](r1))
      RecordMatcher[RepeatedNested](r1, r2) shouldBe true
    }
  }

}
