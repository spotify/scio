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

package com.spotify.scio.avro.types

import com.google.protobuf.ByteString
import org.scalacheck._
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalactic.Equality

// Manual implementation of the required Gen instances.
// Technically, those can be derived automatically using scalacheck-shapeless,
// but automatic derivation takes forever.
private object Generators {
  import Schemas._

  private val bsGen = Gen.alphaStr.map(ByteString.copyFromUtf8)

  implicit val genBasicFields: Gen[BasicFields] =
    for {
      b <- Gen.oneOf(true, false)
      i <- Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE)
      l <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
      f <- Gen.chooseNum(Float.MinValue, Float.MaxValue)
      d <- Gen.chooseNum(Double.MinValue, Double.MaxValue)
      s <- Gen.alphaNumStr
      bs <- bsGen
    } yield BasicFields(b, i, l, f, d, s, bs)

  implicit val genOptionalFields: Gen[OptionalFields] =
    for {
      b <- Gen.option(Gen.oneOf(true, false))
      i <- Gen.option(Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE))
      l <- Gen.option(Gen.chooseNum(Long.MinValue, Long.MaxValue))
      f <- Gen.option(Gen.chooseNum(Float.MinValue, Float.MaxValue))
      d <- Gen.option(Gen.chooseNum(Double.MinValue, Double.MaxValue))
      s <- Gen.option(Gen.alphaNumStr)
      bs <- Gen.option(bsGen)
    } yield OptionalFields(b, i, l, f, d, s, bs)

  implicit val genArrayFields: Gen[ArrayFields] =
    for {
      b <- Gen.listOf(Gen.oneOf(true, false))
      i <- Gen.listOf(Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE))
      l <- Gen.listOf(Gen.chooseNum(Long.MinValue, Long.MaxValue))
      f <- Gen.listOf(Gen.chooseNum(Float.MinValue, Float.MaxValue))
      d <- Gen.listOf(Gen.chooseNum(Double.MinValue, Double.MaxValue))
      s <- Gen.listOf(Gen.alphaNumStr)
      bs <- Gen.listOf(bsGen)
    } yield ArrayFields(b, i, l, f, d, s, bs)

  private def map[T](g: Gen[T]): Gen[Map[String, T]] = {
    val genKV =
      for {
        k <- Gen.alphaNumStr
        v <- g
      } yield (k, v)
    Gen.mapOf(genKV)
  }

  implicit val genMapFields: Gen[MapFields] =
    for {
      b <- map(Gen.oneOf(true, false))
      i <- map(Gen.chooseNum(Integer.MIN_VALUE, Integer.MAX_VALUE))
      l <- map(Gen.chooseNum(Long.MinValue, Long.MaxValue))
      f <- map(Gen.chooseNum(Float.MinValue, Float.MaxValue))
      d <- map(Gen.chooseNum(Double.MinValue, Double.MaxValue))
      s <- map(Gen.alphaNumStr)
      bs <- map(bsGen)
    } yield MapFields(b, i, l, f, d, s, bs)

  implicit val genNestedFields: Gen[NestedFields] =
    for {
      b <- genBasicFields
      o <- genOptionalFields
      a <- genArrayFields
      m <- genMapFields
    } yield NestedFields(b, o, a, m)

  implicit val genOptionalNestedFields: Gen[OptionalNestedFields] =
    for {
      b <- Gen.option(genBasicFields)
      o <- Gen.option(genOptionalFields)
      a <- Gen.option(genArrayFields)
      m <- Gen.option(genMapFields)
    } yield OptionalNestedFields(b, o, a, m)

  implicit val genArrayNestedFields: Gen[ArrayNestedFields] =
    for {
      b <- Gen.listOf(genBasicFields)
      o <- Gen.listOf(genOptionalFields)
      a <- Gen.listOf(genArrayFields)
      m <- Gen.listOf(genMapFields)
    } yield ArrayNestedFields(b, o, a, m)

  implicit val genMapNestedFields: Gen[MapNestedFields] =
    for {
      b <- map(genBasicFields)
      o <- map(genOptionalFields)
      a <- map(genArrayFields)
      m <- map(genMapFields)
    } yield MapNestedFields(b, o, a, m)

  private val bytesGen: Gen[Array[Byte]] = bsGen.map(_.toByteArray())

  implicit val genByteArrayFields: Gen[ByteArrayFields] =
    for {
      r <- bytesGen
      o <- Gen.option(bytesGen)
      l <- Gen.listOf(bytesGen)
    } yield ByteArrayFields(r, o, l)

  implicit def arb[T](implicit gen: Gen[T]): Arbitrary[T] = Arbitrary.apply(gen)
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

  implicit val eqByteArrayFields =
    new Eq[ByteArrayFields] {
      def eqv(a: ByteArrayFields, b: ByteArrayFields): Boolean =
        Eq[Array[Byte]].eqv(a.required, b.required) &&
          Eq[Option[Array[Byte]]].eqv(a.optional, b.optional) &&
          Eq[List[Array[Byte]]].eqv(a.repeated, b.repeated)
    }

  implicit def catsEquality[A: Eq]: Equality[A] =
    new Equality[A] {
      def areEqual(a: A, b: Any): Boolean =
        b.isInstanceOf[A @unchecked] && Eq[A].eqv(a, b.asInstanceOf[A])
    }

}

class ConverterProviderSpec extends PropSpec with ScalaCheckDrivenPropertyChecks with Matchers {

  // TODO: remove this once https://github.com/scalatest/scalatest/issues/1090 is addressed
  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  import Schemas._
  import Generators._
  import Equalities._

  implicit def compareByteArrays(x: Array[Byte], y: Array[Byte]): Boolean =
    ByteString.copyFrom(x) == ByteString.copyFrom(y)

  property("round trip basic primitive types") {
    forAll { r1: BasicFields =>
      val r2 = AvroType.fromGenericRecord[BasicFields](AvroType.toGenericRecord[BasicFields](r1))
      r1 should ===(r2)
    }
  }

  property("round trip optional primitive types") {
    forAll { r1: OptionalFields =>
      val r2 =
        AvroType.fromGenericRecord[OptionalFields](AvroType.toGenericRecord[OptionalFields](r1))
      r1 should ===(r2)
    }
  }

  property("skip null optional primitive types") {
    forAll { o: OptionalFields =>
      val r = AvroType.toGenericRecord[OptionalFields](o)
      // GenericRecord object should only contain a key if the corresponding Option[T] is defined
      o.boolF.isDefined shouldBe (r.get("boolF") != null)
      o.intF.isDefined shouldBe (r.get("intF") != null)
      o.longF.isDefined shouldBe (r.get("longF") != null)
      o.floatF.isDefined shouldBe (r.get("floatF") != null)
      o.doubleF.isDefined shouldBe (r.get("doubleF") != null)
      o.stringF.isDefined shouldBe (r.get("stringF") != null)
      o.byteStringF.isDefined shouldBe (r.get("byteStringF") != null)
    }
  }

  property("round trip primitive type arrays") {
    forAll { r1: ArrayFields =>
      val r2 = AvroType.fromGenericRecord[ArrayFields](AvroType.toGenericRecord[ArrayFields](r1))
      r1 should ===(r2)
    }
  }

  property("round trip primitive type maps") {
    forAll { r1: MapFields =>
      val r2 = AvroType.fromGenericRecord[MapFields](AvroType.toGenericRecord[MapFields](r1))

      r1 should ===(r2)
    }
  }

  property("round trip required nested types") {
    forAll { r1: NestedFields =>
      val r2 = AvroType.fromGenericRecord[NestedFields](AvroType.toGenericRecord[NestedFields](r1))
      r1 should ===(r2)
    }
  }

  property("round trip optional nested types") {
    forAll { r1: OptionalNestedFields =>
      val r2 = AvroType.fromGenericRecord[OptionalNestedFields](
        AvroType.toGenericRecord[OptionalNestedFields](r1)
      )
      r1 should ===(r2)
    }
  }

  property("skip null optional nested types") {
    forAll { o: OptionalNestedFields =>
      val r = AvroType.toGenericRecord[OptionalNestedFields](o)
      // TableRow object should only contain a key if the corresponding Option[T] is defined
      o.basic.isDefined shouldBe (r.get("basic") != null)
      o.optional.isDefined shouldBe (r.get("optional") != null)
      o.array.isDefined shouldBe (r.get("array") != null)
      o.map.isDefined shouldBe (r.get("map") != null)
    }
  }

  property("round trip nested type arrays") {
    forAll { r1: ArrayNestedFields =>
      val r2 = AvroType.fromGenericRecord[ArrayNestedFields](
        AvroType.toGenericRecord[ArrayNestedFields](r1)
      )
      r1 should ===(r2)
    }
  }

  property("round trip nested type maps") {
    forAll { r1: MapNestedFields =>
      val r2 =
        AvroType.fromGenericRecord[MapNestedFields](AvroType.toGenericRecord[MapNestedFields](r1))
      r1 should ===(r2)
    }
  }

  property("round trip byte array types") {
    forAll { r1: ByteArrayFields =>
      val r2 =
        AvroType.fromGenericRecord[ByteArrayFields](AvroType.toGenericRecord[ByteArrayFields](r1))
      r1 should ===(r2)
    }
  }

}
