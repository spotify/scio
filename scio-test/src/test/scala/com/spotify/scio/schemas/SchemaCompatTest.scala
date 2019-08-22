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
package com.spotify.scio.schemas

import org.scalatest._

object SchemaCompatTest {
  case class Required(i: Int, s: String)
  case class Nullable(i: Option[Int], s: Option[String])
  case class Repeated(i: List[Int], s: List[String], n: List[Required])
  case class MapField(i: Map[String, Int], s: Map[String, String], n: Map[String, Required])
  case class Nested(a: Required, b: Nullable, c: Repeated, d: MapField)

  // projected schemas
  case class RequiredP(i: Int)
  case class NullableP(i: Option[Int])
  case class RepeatedP(i: List[Int], n: List[RequiredP])
  case class MapFieldP(i: Map[String, Int], n: Map[String, RequiredP])
  case class NestedP(a: RequiredP, b: NullableP, c: RepeatedP, d: MapFieldP)
}

class SchemaCompatTest extends FlatSpec with Matchers {

  import com.spotify.scio.schemas.SchemaCompatTest._

  private def check[A: Schema, B: Schema]: Either[String, Unit] = {
    val a = SchemaMaterializer.beamSchema(Schema[A])
    val b = SchemaMaterializer.beamSchema(Schema[B])
    To.checkCompatibility(a, b)(())
  }

  "To.checkCompatibility" should "pass for identical schemas" in {
    check[Required, Required] shouldBe Right(())
    check[Nullable, Nullable] shouldBe Right(())
    check[Repeated, Repeated] shouldBe Right(())
    check[MapField, MapField] shouldBe Right(())
    check[Nested, Nested] shouldBe Right(())
  }

  it should "fail incompatible schemas" in {
    check[Required, Nullable] should be('left)
    check[Required, Repeated] should be('left)
    check[Required, MapField] should be('left)
  }

  it should "support projection" in {
    check[Required, RequiredP] shouldBe Right(())
    check[RequiredP, Required] should be('left)

    check[Nullable, NullableP] shouldBe Right(())
    check[NullableP, Nullable] should be('left)

    check[Repeated, RepeatedP] shouldBe Right(())
    check[RepeatedP, Repeated] should be('left)

    check[MapField, MapFieldP] shouldBe Right(())
    check[MapFieldP, MapField] should be('left)

    check[Nested, NestedP] shouldBe Right(())
    check[NestedP, Nested] should be('left)
  }
}
