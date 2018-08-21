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

package com.spotify.scio.jmh

import java.lang.{Iterable => JIterable}
import java.util.concurrent.TimeUnit

import com.google.common.collect.AbstractIterator
import com.google.common.collect.Lists
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._

import com.spotify.scio.bigquery.types._

// scalastyle:off number.of.methods
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class BigQueryBenchmark {
  import BigQueryBenchmark.Simple
  
  val s = Simple.toTableRow(Simple("test"))
  
  @Benchmark def bq(): Simple = Simple.fromTableRow(s)

}
// scalastyle:on number.of.methods
object BigQueryBenchmark {
  @BigQueryType.toTable 
  case class Simple(a: String)
}
