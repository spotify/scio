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

package com.spotify.scio.jmh

import org.openjdk.jmh.annotations._
import org.scalacheck._
import magnolify.scalacheck.auto._
import java.util.concurrent.TimeUnit
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord


object MacroBenchmark {

  case class Simple(b: Boolean, i: Int, s: String)
  case class Repeated(b: List[Boolean], i: List[Int], s: List[String])
  case class Nested(
                     b: Boolean,
                     i: Int,
                     s: String,
                     r: Simple,
                     o: Option[Simple],
                     l: List[Simple]
                   )

  val seed: rng.Seed = rng.Seed(0)
  val prms: Gen.Parameters = Gen.Parameters.default
  val nested: Nested = implicitly[Arbitrary[Nested]].arbitrary(prms, seed).get
}

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class MagnolifyMacroBench {
  import MacroBenchmark._
  import magnolify.avro._

  val avroType = AvroType[Nested]
  val genericRecord = avroType.to(nested)

  @Benchmark def avroTo: GenericRecord = avroType.to(nested)
  @Benchmark def avroFrom: Nested = avroType.from(genericRecord)
  @Benchmark def avroSchema: Schema = AvroType[Nested].schema
}

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class ScioMacroBench {
  import MacroBenchmark._
  import com.spotify.scio.avro.types._

  val avroType = AvroType[Nested]
  val genericRecord = avroType.toGenericRecord(nested)

  @Benchmark def avroTo: GenericRecord = avroType.toGenericRecord(nested)
  @Benchmark def avroFrom: Nested = avroType.fromGenericRecord(genericRecord)
  @Benchmark def avroSchema: Schema = AvroType[Nested].schema
}
