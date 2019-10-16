/*
 * Copyright 2019 Spotify AB.
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

import java.math.MathContext

import com.google.protobuf.ByteString
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.scalacheck._
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.spotify.scio.bigquery.Numeric
import org.scalactic.Equality

// Manual implementation of the required Gen instances.
// Technically, those can be derived automatically using scalacheck-shapeless,
// but automatic derivation takes forever.
private object Generators {
  import Schemas._

  private val bsGen = Gen.alphaStr.map(ByteString.copyFromUtf8)
  private val bytesGen: Gen[Array[Byte]] = bsGen.map(_.toByteArray())
  implicit def arb[T](implicit gen: Gen[T]): Arbitrary[T] = Arbitrary.apply(gen)

  private val genByteArray = Gen.alphaStr.map(_.getBytes)
  private val genByteString = Gen.alphaStr.map(ByteString.copyFromUtf8)
  private val genInstant = Gen.const(Instant.now())
  private val genDate = Gen.const(LocalDate.now())
  private val genTime = Gen.const(LocalTime.now())
  private val genDatetime = Gen.const(LocalDateTime.now())
  private val genNumericBigDecimal =
    for {
      bd <- Arbitrary.arbitrary[BigDecimal]
    } yield {
      val rounded = BigDecimal(bd.toString(), new MathContext(Numeric.MaxNumericPrecision))
      Numeric(rounded)
    }

  implicit val genRequired: Gen[Required] =
    for {
      b <- Gen.oneOf(true, false)
      i <- Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE)
      l <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
      f <- Gen.chooseNum(Float.MinValue, Float.MaxValue)
      d <- Gen.chooseNum(Double.MinValue, Double.MaxValue)
      s <- Gen.alphaNumStr
      by <- bytesGen
      bs <- bsGen
      ins <- genInstant
      dat <- genDate
      tim <- genTime
      dtt <- genDatetime
      big <- genNumericBigDecimal
    } yield Required(b, i, l, f, d, s, by, bs, ins, dat, tim, dtt, big)

  implicit val genOptional: Gen[Optional] =
    for {
      b <- Gen.option(Gen.oneOf(true, false))
      i <- Gen.option(Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE))
      l <- Gen.option(Gen.chooseNum(Long.MinValue, Long.MaxValue))
      f <- Gen.option(Gen.chooseNum(Float.MinValue, Float.MaxValue))
      d <- Gen.option(Gen.chooseNum(Double.MinValue, Double.MaxValue))
      s <- Gen.option(Gen.alphaNumStr)
      by <- Gen.option(bytesGen)
      bs <- Gen.option(bsGen)
      ins <- Gen.option(genInstant)
      dat <- Gen.option(genDate)
      tim <- Gen.option(genTime)
      dtt <- Gen.option(genDatetime)
      big <- Gen.option(genNumericBigDecimal)
    } yield Optional(b, i, l, f, d, s, by, bs, ins, dat, tim, dtt, big)

  implicit val genRepeated: Gen[Repeated] =
    for {
      b <- Gen.listOf(Gen.oneOf(true, false))
      i <- Gen.listOf(Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE))
      l <- Gen.listOf(Gen.chooseNum(Long.MinValue, Long.MaxValue))
      f <- Gen.listOf(Gen.chooseNum(Float.MinValue, Float.MaxValue))
      d <- Gen.listOf(Gen.chooseNum(Double.MinValue, Double.MaxValue))
      s <- Gen.listOf(Gen.alphaNumStr)
      by <- Gen.listOf(bytesGen)
      bs <- Gen.listOf(bsGen)
      ins <- Gen.listOf(genInstant)
      dat <- Gen.listOf(genDate)
      tim <- Gen.listOf(genTime)
      dtt <- Gen.listOf(genDatetime)
      big <- Gen.listOf(genNumericBigDecimal)
    } yield Repeated(b, i, l, f, d, s, by, bs, ins, dat, tim, dtt, big)

  implicit val genNested: Gen[RequiredNested] =
    for {
      r <- genRequired
      o <- genOptional
      rs <- genRepeated
    } yield RequiredNested(r, o, rs)

  implicit val genOptionalNested: Gen[OptionalNested] =
    for {
      r <- Gen.option(genRequired)
      o <- Gen.option(genOptional)
      rs <- Gen.option(genRepeated)
    } yield OptionalNested(r, o, rs)

  implicit val genRepeatedNested: Gen[RepeatedNested] =
    for {
      r <- Gen.listOf(genRequired)
      o <- Gen.listOf(genOptional)
      rs <- Gen.listOf(genRepeated)
    } yield RepeatedNested(r, o, rs)
}

object Equalities {
  import Schemas._

  // Scalatest Equality trait is rubbish so we use cats instead
  import cats.kernel.Eq

  implicit val equalityArrayByte =
    new Eq[Array[Byte]] {
      def eqv(x: Array[Byte], y: Array[Byte]): Boolean =
        ByteString.copyFrom(x) == ByteString.copyFrom(y)
    }

  implicit def equalityOption[A: Eq]: Eq[Option[A]] =
    new Eq[Option[A]] {
      def eqv(a: Option[A], b: Option[A]): Boolean =
        (a, b) match {
          case (None, None)       => true
          case (Some(a), Some(b)) => Eq[A].eqv(a, b)
          case _                  => false
        }
    }

  implicit def equalityList[A: Eq]: Eq[List[A]] =
    new Eq[List[A]] {
      def eqv(as: List[A], bs: List[A]): Boolean =
        (as.length == bs.length) &&
          as.zip(bs).forall { case (a, b) => Eq[A].eqv(a, b) }
    }

  // scalastyle:off cyclomatic.complexity
  implicit val equalityRequired =
    new Eq[Required] {
      def eqv(a: Required, b: Required): Boolean = {
        a.boolF == b.boolF &&
        a.intF == b.intF &&
        a.longF == b.longF &&
        a.floatF == b.floatF &&
        a.doubleF == b.doubleF &&
        a.stringF == b.stringF &&
        Eq[Array[Byte]].eqv(a.byteArrayF, b.byteArrayF) &&
        a.byteStringF == b.byteStringF &&
        a.timestampF == b.timestampF &&
        a.dateF == b.dateF &&
        a.timeF == b.timeF &&
        a.datetimeF == b.datetimeF &&
        a.bigDecimalF == b.bigDecimalF
      }
    }

  implicit val equalityOptional =
    new Eq[Optional] {
      def eqv(a: Optional, b: Optional): Boolean = {
        a.boolF == b.boolF &&
        a.intF == b.intF &&
        a.longF == b.longF &&
        a.floatF == b.floatF &&
        a.doubleF == b.doubleF &&
        a.stringF == b.stringF &&
        Eq[Option[Array[Byte]]].eqv(a.byteArrayF, b.byteArrayF) &&
        a.byteStringF == b.byteStringF &&
        a.timestampF == b.timestampF &&
        a.dateF == b.dateF &&
        a.timeF == b.timeF &&
        a.datetimeF == b.datetimeF &&
        a.bigDecimalF == b.bigDecimalF
      }
    }

  implicit val equalityRepeated =
    new Eq[Repeated] {
      def eqv(a: Repeated, b: Repeated): Boolean = {
        a.boolF == b.boolF &&
        a.intF == b.intF &&
        a.longF == b.longF &&
        a.floatF == b.floatF &&
        a.doubleF == b.doubleF &&
        a.stringF == b.stringF &&
        Eq[List[Array[Byte]]].eqv(a.byteArrayF, b.byteArrayF) &&
        a.byteStringF == b.byteStringF &&
        a.timestampF == b.timestampF &&
        a.dateF == b.dateF &&
        a.timeF == b.timeF &&
        a.datetimeF == b.datetimeF &&
        a.bigDecimalF == b.bigDecimalF
      }
    }
  // scalastyle:on cyclomatic.complexity

  implicit val equalityRequiredNested =
    new Eq[RequiredNested] {
      def eqv(a: RequiredNested, b: RequiredNested): Boolean = {
        Eq[Required].eqv(a.required, b.required) &&
        Eq[Optional].eqv(a.optional, b.optional) &&
        Eq[Repeated].eqv(a.repeated, b.repeated)
      }
    }

  implicit val equalityOptionalNested =
    new Eq[OptionalNested] {
      def eqv(a: OptionalNested, b: OptionalNested): Boolean =
        Eq[Option[Required]].eqv(a.required, b.required) &&
          Eq[Option[Optional]].eqv(a.optional, b.optional) &&
          Eq[Option[Repeated]].eqv(a.repeated, b.repeated)
    }

  implicit val equalityRepeatedNested =
    new Eq[RepeatedNested] {
      def eqv(a: RepeatedNested, b: RepeatedNested): Boolean =
        Eq[List[Required]].eqv(a.required, b.required) &&
          Eq[List[Optional]].eqv(a.optional, b.optional) &&
          Eq[List[Repeated]].eqv(a.repeated, b.repeated)
    }


  implicit def catsEquality[A: Eq]: Equality[A] =
    new Equality[A] {
      def areEqual(a: A, b: Any): Boolean =
        b.isInstanceOf[A @unchecked] && Eq[A].eqv(a, b.asInstanceOf[A])
    }
}

final class ConverterProviderSpec
    extends PropSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers {

  // TODO: remove this once https://github.com/scalatest/scalatest/issues/1090 is addressed
  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  import Schemas._
  import Generators._
  import Equalities._

  implicit def compareByteArrays(x: Array[Byte], y: Array[Byte]): Boolean =
    ByteString.copyFrom(x) == ByteString.copyFrom(y)

  property("round trip required primitive types") {
    forAll { r1: Required =>
      val r2 = BigQueryType.fromTableRow[Required](BigQueryType.toTableRow[Required](r1))
      r1 should ===(r2)
    }
  }

  property("round trip optional primitive types") {
    forAll { r1: Optional =>
      val r2 = BigQueryType.fromTableRow[Optional](BigQueryType.toTableRow[Optional](r1))
      r1 should ===(r2)
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
      o.bigDecimalF.isDefined shouldBe r.containsKey("bigDecimalF")
    }
  }

  property("round trip repeated primitive types") {
    forAll { r1: Repeated =>
      val r2 = BigQueryType.fromTableRow[Repeated](BigQueryType.toTableRow[Repeated](r1))
      r1 should ===(r2)
    }
  }

  property("round trip required nested types") {
    forAll { r1: RequiredNested =>
      val r2 =
        BigQueryType.fromTableRow[RequiredNested](BigQueryType.toTableRow[RequiredNested](r1))
      r1 should ===(r2)
    }
  }

  property("round trip optional nested types") {
    forAll { r1: OptionalNested =>
      val r2 =
        BigQueryType.fromTableRow[OptionalNested](BigQueryType.toTableRow[OptionalNested](r1))
      r1 should ===(r2)
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
      val r2 =
        BigQueryType.fromTableRow[RepeatedNested](BigQueryType.toTableRow[RepeatedNested](r1))
      r1 should ===(r2)
    }
  }

}
