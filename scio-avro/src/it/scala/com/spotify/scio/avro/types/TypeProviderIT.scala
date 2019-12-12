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

class TypeProviderIT extends AnyFlatSpec with Matchers {

  @AvroType.fromSchemaFile("""
      |https://raw.githubusercontent.com/spotify/scio/master/
      |scio-avro/src/test/avro/
      |scio-avro-test.avsc
    """.stripMargin)
  class FromResourceMultiLine
  @AvroType.fromSchemaFile(
    "https://raw.githubusercontent.com/spotify/scio/master/scio-avro/src/test/avro/scio-avro-test.avsc"
  )
  class FromResource

  "AvroType.fromSchemaFile" should "support reading schema from multiline resource" in {
    val r = FromResourceMultiLine(1)
    r.test shouldBe 1
  }

  it should "support reading schema from resource" in {
    val r = FromResource(2)
    r.test shouldBe 2
  }

  class Annotation1 extends StaticAnnotation
  class Annotation2 extends StaticAnnotation

  @Annotation1
  @AvroType.fromSchemaFile(
    "https://raw.githubusercontent.com/spotify/scio/master/scio-avro/src/test/avro/scio-avro-test.avsc"
  )
  @Annotation2
  class FromResourceWithSurroundingAnnotations

  it should "preserve surrounding user defined annotations" in {
    containsAllAnnotTypes[FromResourceWithSurroundingAnnotations]
  }
  @AvroType.fromSchemaFile(
    "https://raw.githubusercontent.com/spotify/scio/master/scio-avro/src/test/avro/scio-avro-test.avsc"
  )
  @Annotation1
  @Annotation2
  class FromResourceWithSequentialAnnotations

  it should "preserve sequential user defined annotations" in {
    containsAllAnnotTypes[FromResourceWithSequentialAnnotations]
  }

  def containsAllAnnotTypes[T: TypeTag]: Assertion =
    typeOf[T].typeSymbol.annotations
      .map(_.tree.tpe)
      .containsSlice(Seq(typeOf[Annotation1], typeOf[Annotation2])) shouldBe true

}
