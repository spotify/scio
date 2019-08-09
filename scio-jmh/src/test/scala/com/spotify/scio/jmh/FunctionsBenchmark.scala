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

import java.util
import java.util.concurrent.TimeUnit

import com.spotify.scio.ScioContext
import com.spotify.scio.util.Functions
import com.twitter.algebird.{Monoid, Semigroup}
import org.apache.beam.sdk.testing.CombineFnTester
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class FunctionsBenchmark {

  type T = Set[Int]

  val input = new util.ArrayList((1 to 100).map(Set(_)).asJava)
  val output = (1 to 100).toSet

  val aggregateFn =
    Functions.aggregateFn[T, T](ScioContext(), Set.empty[Int])(_ ++ _, _ ++ _)
  val combineFn =
    Functions.combineFn[T, T](ScioContext(), identity, _ ++ _, _ ++ _)
  val reduceFn = Functions.reduceFn(ScioContext(), (x: T, y: T) => x ++ y)
  val sgFn = Functions.reduceFn(ScioContext(), Semigroup.setSemigroup[Int])
  val monFn = Functions.reduceFn(ScioContext(), Monoid.setMonoid[Int])

  def test(fn: CombineFn[T, _, T], input: java.util.List[T], output: T): T = {
    CombineFnTester.testCombineFn(fn, input, output)
    output
  }

  @Benchmark def benchAggregate: T = test(aggregateFn, input, output)
  @Benchmark def benchCombine: T = test(combineFn, input, output)
  @Benchmark def benchReduce: T = test(reduceFn, input, output)
  @Benchmark def benchSemigroup: T = test(sgFn, input, output)
  @Benchmark def benchMonoid: T = test(monFn, input, output)

}
