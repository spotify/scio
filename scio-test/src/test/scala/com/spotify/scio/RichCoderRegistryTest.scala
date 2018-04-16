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

package com.spotify.scio

import com.google.protobuf.Timestamp
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.avro.Account
import com.spotify.scio.coders.CoderTestUtils._
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.coders.CoderRegistry
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.TestPipeline
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.reflect.ClassTag

class RichCoderRegistryTest extends PipelineSpec {

  import Implicits._

  val pipeline = TestPipeline.create()
  val registry = pipeline.getCoderRegistry
  val options = PipelineOptionsFactory.create()
  registry.registerScalaCoders()

  private def roundTrip[T: ClassTag](value: T) = new Matcher[CoderRegistry] {
    override def apply(left: CoderRegistry): MatchResult = {
      val coder = left.getScalaCoder[T](options)
      coder should not be null
      MatchResult(
        testRoundTrip(coder, value),
        s"CoderRegistry did not round trip $value",
        s"CoderRegistry did round trip $value")
    }
  }

  "RichCoderRegistry" should "support Scala primitives" in {
    registry should roundTrip (10)
    registry should roundTrip (10L)
    registry should roundTrip (10F)
    registry should roundTrip (10.0)
  }

  it should "support Scala tuples" in {
    registry should roundTrip (("hello", 10))
    registry should roundTrip (("hello", 10, 10.0))
    registry should roundTrip (("hello", (10, 10.0)))
  }

  it should "support Scala case classes" in {
    registry should roundTrip (Pair("record", 10))
  }

  it should "support Avro GenericRecord" in {
    val r = newGenericRecord(1)
    registry should roundTrip (r)
    registry should roundTrip (("key", r))
    registry should roundTrip (CaseClassWithGenericRecord("record", 10, r))
  }

  it should "support Avro SpecificRecord" in {
    val r = newSpecificRecord(1)
    registry should roundTrip (r)
    registry should roundTrip (("key", r))
    registry should roundTrip (CaseClassWithSpecificRecord("record", 10, r))
  }

  it should "support Avro SpecificRecord in joins" in {
    val expected = Seq(
      new Account(1, "checking", "Alice", 1000.0),
      new Account(2, "checking", "Bob", 2000.0))

    runWithContext { sc =>
      val lhs = sc.parallelize(1 to 10).map(i => new Account(i, "checking", "u" + i, i * 1000.0))
      val rhs = sc.parallelize(Seq(1 -> "Alice", 2 -> "Bob"))
      lhs
        .keyBy(_.getId.toInt)
        .join(rhs)
        .mapValues { case (account, name) => Account.newBuilder(account).setName(name).build() }
        .values should containInAnyOrder (expected)
    }
  }

  it should "give ProtoCoder higher precedence" in {
    // FIXME: BEAM-2658 make sure ProtoCoder has higher precedence than SerializableCoder
    registry.getScalaCoder[Timestamp](options) shouldBe ProtoCoder.of(classOf[Timestamp])
  }

}
