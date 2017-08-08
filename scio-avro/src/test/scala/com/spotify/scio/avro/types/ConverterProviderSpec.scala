/*
 * Copyright 2017 Spotify AB.
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
import shapeless.datatype.record._
import org.scalacheck._
import org.scalacheck.ScalacheckShapeless._
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class ConverterProviderSpec extends PropSpec with GeneratorDrivenPropertyChecks with Matchers {

  // TODO: remove this once https://github.com/scalatest/scalatest/issues/1090 is addressed
  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  import Schemas._

  implicit val arbByteString = Arbitrary(Gen.alphaStr.map(ByteString.copyFromUtf8))

  property("round trip basic primitive types") {
    forAll { r1: BasicFields =>
      val r2 = AvroType.fromGenericRecord[BasicFields](AvroType.toGenericRecord[BasicFields](r1))
      RecordMatcher[BasicFields](r1, r2) shouldBe true
    }
  }


  property("round trip optional primitive types") {
    forAll { r1: OptionalFields =>
      val r2 = AvroType.fromGenericRecord[OptionalFields](
        AvroType.toGenericRecord[OptionalFields](r1))
      RecordMatcher[OptionalFields](r1, r2) shouldBe true
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
      RecordMatcher[ArrayFields](r1, r2) shouldBe true
    }
  }

  property("round trip primitive type maps") {
    forAll { r1: MapFields =>
      val r2 = AvroType.fromGenericRecord[MapFields](AvroType.toGenericRecord[MapFields](r1))

      RecordMatcher[MapFields](r1, r2) shouldBe true
    }
  }

  property("round trip required nested types") {
    forAll { r1: NestedFields =>
      val r2 = AvroType.fromGenericRecord[NestedFields](
        AvroType.toGenericRecord[NestedFields](r1))
      RecordMatcher[NestedFields](r1, r2) shouldBe true
    }
  }

  property("round trip optional nested types") {
    forAll { r1: OptionalNestedFields =>
      val r2 = AvroType.fromGenericRecord[OptionalNestedFields](
        AvroType.toGenericRecord[OptionalNestedFields](r1))
      RecordMatcher[OptionalNestedFields](r1, r2) shouldBe true
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
        AvroType.toGenericRecord[ArrayNestedFields](r1))
      RecordMatcher[ArrayNestedFields](r1, r2) shouldBe true
    }
  }

  property("round trip nested type maps") {
    forAll { r1: MapNestedFields =>
      val r2 = AvroType.fromGenericRecord[MapNestedFields](
        AvroType.toGenericRecord[MapNestedFields](r1))
      RecordMatcher[MapNestedFields](r1, r2) shouldBe true
    }
  }

}
