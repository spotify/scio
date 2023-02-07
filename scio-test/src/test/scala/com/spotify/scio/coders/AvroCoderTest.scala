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

package com.spotify.scio.coders

import org.scalatest.flatspec.AnyFlatSpec
import com.spotify.scio.testing.CoderAssertions._
import org.apache.avro.generic.GenericRecord
import org.scalactic.Equality
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._

final class AvroCoderTest extends AnyFlatSpec with Matchers {
  object Avro {

    import com.spotify.scio.avro.{Account, Address, User => AvUser}

    val accounts: List[Account] = List(new Account(1, "type", "name", 12.5, null))
    val address =
      new Address("street1", "street2", "city", "state", "01234", "Sweden")
    val user = new AvUser(1, "lastname", "firstname", "email@foobar.com", accounts.asJava, address)

    val eq: Equality[GenericRecord] = (a: GenericRecord, b: Any) => a.toString === b.toString
  }

  it should "support Avro's SpecificRecordBase" in {
    Avro.user coderShould notFallback()
  }

  it should "support Avro's GenericRecord" in {
    val schema = Avro.user.getSchema
    val record: GenericRecord = Avro.user

    implicit val c: Coder[GenericRecord] = Coder.avroGenericRecordCoder(schema)
    implicit val eq: Equality[GenericRecord] = Avro.eq

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
