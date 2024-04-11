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

import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._

object TypeProviderIT {
  private val SchemaFile =
    "https://raw.githubusercontent.com/spotify/scio/master/integration/src/test/avro/avro-type-provider.avsc"

  @AvroType.fromSchemaFile(SchemaFile)
  class FromResourceMultiLine
  @AvroType.fromSchemaFile(SchemaFile)
  class FromResource

  class Annotation1 extends StaticAnnotation
  class Annotation2 extends StaticAnnotation

  @Annotation1
  @AvroType.fromSchemaFile(SchemaFile)
  @Annotation2
  class FromResourceWithSurroundingAnnotations

  @AvroType.fromSchemaFile(SchemaFile)
  @Annotation1
  @Annotation2
  class FromResourceWithSequentialAnnotations
}

class TypeProviderIT extends AnyFlatSpec with Matchers {

  import TypeProviderIT._

  "AvroType.fromSchemaFile" should "support reading schema from multiline resource" in {
    val r = FromResourceMultiLine(1)
    r.test shouldBe 1
  }

  it should "support reading schema from resource" in {
    val r = FromResource(2)
    r.test shouldBe 2
  }

  it should "preserve surrounding user defined annotations" in {
    containsAllAnnotTypes[FromResourceWithSurroundingAnnotations]
  }

  it should "preserve sequential user defined annotations" in {
    containsAllAnnotTypes[FromResourceWithSequentialAnnotations]
  }

  private def containsAllAnnotTypes[T: TypeTag]: Assertion =
    typeOf[T].typeSymbol.annotations
      .map(_.tree.tpe)
      .containsSlice(Seq(typeOf[Annotation1], typeOf[Annotation2])) shouldBe true

}
