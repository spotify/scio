/*
 * Copyright 2021 Spotify AB.
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

import org.scalatest.flatspec.AnyFlatSpec
import com.spotify.scio.testing.CoderAssertions._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.scalactic.Equality
import org.scalatest.matchers.should.Matchers

final class AvroCoderTest extends AnyFlatSpec with Matchers {

  it should "support Avro's SpecificRecord" in {
    Avro.user coderShould notFallback()
  }

  it should "support not Avro's SpecificRecord if a concrete type is not provided" in {
    val caught = intercept[RuntimeException] {
      Avro.user.asInstanceOf[SpecificRecord] coderShould notFallback()
    }

    caught.getMessage should startWith(
      "Failed to create a coder for SpecificRecord because it is impossible to retrieve an Avro"
    )
  }

  it should "support avrohugger generated SpecificRecord" in {
    Avro.scalaSpecificAvro coderShould notFallback()
  }

  it should "support Avro's GenericRecord" in {
    val schema = Avro.user.getSchema
    val record: GenericRecord = Avro.user

    implicit val c: Coder[GenericRecord] = Coder.avroGenericRecordCoder(schema)
    implicit val eq: Equality[GenericRecord] =
      (a: GenericRecord, b: Any) => a.toString === b.toString

    record coderShould notFallback()
  }

  it should "provide a fallback for GenericRecord if no safe coder is available" in {
    val record: GenericRecord = Avro.user
    record coderShould fallback()
  }

  it should "support specific fixed data" in {
    val bytes = (0 to 15).map(_.toByte).toArray
    val specificFixed = new FixedSpecificDataExample(bytes)
    specificFixed coderShould beDeterministic() and roundtrip()
  }
}
