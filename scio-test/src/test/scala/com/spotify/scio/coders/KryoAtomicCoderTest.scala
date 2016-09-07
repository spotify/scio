/*
 * Copyright 2016 Spotify AB.
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

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.values.KV
import com.google.common.collect.ImmutableList
import com.spotify.scio.avro.AvroUtils._
import com.spotify.scio.avro.TestRecord
import com.spotify.scio.coders.CoderTestUtils._
import com.spotify.scio.testing.PipelineSpec
import org.joda.time.Instant
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.reflect.ClassTag

class KryoAtomicCoderTest extends PipelineSpec {

  import com.spotify.scio.testing.TestingUtils._

  type CoderFactory = () => Coder[Any]
  val cf = () => KryoAtomicCoder[Any]

  private def roundTrip[T: ClassTag](value: T) = new Matcher[CoderFactory] {
    override def apply(left: CoderFactory): MatchResult = {
      MatchResult(
        testRoundTrip(left(), left(), value),
        s"Coder did not round trip $value",
        s"Coder did round trip $value")
    }
  }

  "KryoAtomicCoder" should "support Scala collections" in {
    cf should roundTrip (Seq(1, 2, 3))
    cf should roundTrip (List(1, 2, 3))
    cf should roundTrip (Set(1, 2, 3))
    cf should roundTrip (Map("a" -> 1, "b" -> 2, "c" -> 3))
  }

  it should "support Scala tuples" in {
    cf should roundTrip (("hello", 10))
    cf should roundTrip (("hello", 10, 10.0))
    cf should roundTrip (("hello", (10, 10.0)))
  }

  it should "support Scala case classes" in {
    cf should roundTrip (Pair("record", 10))
  }

  it should "support wrapped iterables" in {
    cf should roundTrip (iterable(1, 2, 3))
  }

  it should "support Avro GenericRecord" in {
    val r = newGenericRecord(1)
    cf should roundTrip (r)
    cf should roundTrip (("key", r))
    cf should roundTrip (CaseClassWithGenericRecord("record", 10, r))
  }

  it should "support Avro SpecificRecord" in {
    val r = new TestRecord(1, 1L, 1F, 1.0, true, "hello")
    cf should roundTrip (r)
    cf should roundTrip (("key", r))
    cf should roundTrip (CaseClassWithSpecificRecord("record", 10, r))
  }

  it should "support KV" in {
    cf should roundTrip (KV.of("key", 1.0))
    cf should roundTrip (KV.of("key", (10, 10.0)))
    cf should roundTrip (KV.of("key", newSpecificRecord(1)))
    cf should roundTrip (KV.of("key", newGenericRecord(1)))
  }

  it should "support Instant" in {
    cf should roundTrip (Instant.now())
  }
  it should "support TableRow" in {
    val r = new TableRow().set("repeated_field", ImmutableList.of("a", "b"))
    cf should roundTrip (r)
  }

}
