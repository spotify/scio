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

import cats.Eq
import cats.instances.all._
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto._
import org.scalacheck._
import org.scalatest.propspec.AnyPropSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ConverterProviderSpec extends AnyPropSpec with ScalaCheckDrivenPropertyChecks with Matchers {
  // Default minSuccessful is 10 instead of 100 in ScalaCheck but that should be enough
  // https://github.com/scalatest/scalatest/issues/1090 is addressed

  import Schemas._

  implicit val arbByteArray: Arbitrary[Array[Byte]] = Arbitrary(Gen.alphaStr.map(_.getBytes))
  implicit val arbByteString: Arbitrary[ByteString] = Arbitrary(
    Gen.alphaStr.map(ByteString.copyFromUtf8)
  )
  implicit val eqByteArrays: Eq[Array[Byte]] = Eq.instance[Array[Byte]](_.toList == _.toList)
  implicit val eqByteString: Eq[ByteString] = Eq.instance[ByteString](_ == _)

  property("round trip basic primitive types") {
    forAll { (r1: BasicFields) =>
      val r2 = AvroType.fromGenericRecord[BasicFields](AvroType.toGenericRecord[BasicFields](r1))
      EqGen.of[BasicFields].eqv(r1, r2) shouldBe true
    }
  }

  property("round trip optional primitive types") {
    forAll { (r1: OptionalFields) =>
      val r2 =
        AvroType.fromGenericRecord[OptionalFields](AvroType.toGenericRecord[OptionalFields](r1))
      EqGen.of[OptionalFields].eqv(r1, r2) shouldBe true
    }
  }

  property("skip null optional primitive types") {
    forAll { (o: OptionalFields) =>
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
    forAll { (r1: ArrayFields) =>
      val r2 = AvroType.fromGenericRecord[ArrayFields](AvroType.toGenericRecord[ArrayFields](r1))
      EqGen.of[ArrayFields].eqv(r1, r2) shouldBe true
    }
  }

  property("round trip primitive type maps") {
    forAll { (r1: MapFields) =>
      val r2 = AvroType.fromGenericRecord[MapFields](AvroType.toGenericRecord[MapFields](r1))
      EqGen.of[MapFields].eqv(r1, r2) shouldBe true
    }
  }

  property("round trip required nested types") {
    forAll { (r1: NestedFields) =>
      val r2 = AvroType.fromGenericRecord[NestedFields](AvroType.toGenericRecord[NestedFields](r1))
      EqGen.of[NestedFields].eqv(r1, r2) shouldBe true
    }
  }

  property("round trip optional nested types") {
    forAll { (r1: OptionalNestedFields) =>
      val r2 = AvroType.fromGenericRecord[OptionalNestedFields](
        AvroType.toGenericRecord[OptionalNestedFields](r1)
      )
      EqGen.of[OptionalNestedFields].eqv(r1, r2) shouldBe true
    }
  }

  property("skip null optional nested types") {
    forAll { (o: OptionalNestedFields) =>
      val r = AvroType.toGenericRecord[OptionalNestedFields](o)
      // TableRow object should only contain a key if the corresponding Option[T] is defined
      o.basic.isDefined shouldBe (r.get("basic") != null)
      o.optional.isDefined shouldBe (r.get("optional") != null)
      o.array.isDefined shouldBe (r.get("array") != null)
      o.map.isDefined shouldBe (r.get("map") != null)
    }
  }

  property("round trip nested type arrays") {
    forAll { (r1: ArrayNestedFields) =>
      val r2 = AvroType.fromGenericRecord[ArrayNestedFields](
        AvroType.toGenericRecord[ArrayNestedFields](r1)
      )
      EqGen.of[ArrayNestedFields].eqv(r1, r2) shouldBe true
    }
  }

  // FIXME: can't derive Eq for this
//  property("round trip nested type maps") {
//    forAll { (r1: MapNestedFields) =>
//      val r2 =
//        AvroType.fromGenericRecord[MapNestedFields](AvroType.toGenericRecord[MapNestedFields](r1))
//      EqGen.of[MapNestedFields].eqv(r1, r2) shouldBe true
//    }
//  }

  property("round trip byte array types") {
    forAll { (r1: ByteArrayFields) =>
      val r2 =
        AvroType.fromGenericRecord[ByteArrayFields](AvroType.toGenericRecord[ByteArrayFields](r1))
      EqGen.of[ByteArrayFields].eqv(r1, r2) shouldBe true
    }
  }
}
