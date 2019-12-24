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

import com.spotify.scio.util.{BloomFilter, BloomFilterAggregator, MutableBF}
import org.openjdk.jmh.annotations._

import scala.util.Random

/**
 * Benchmarks for com.spotify.scio.util.BloomFilter
 *
 * Creating a BF from a collection
 */
object BloomFilterCreateBenchmark {
  def createRandomString(nbrOfStrings: Int, lengthOfStrings: Int): Seq[String] =
    Seq.fill(nbrOfStrings)(Random.nextString(lengthOfStrings))

  @State(Scope.Benchmark)
  class BloomFilterState {
    @Param(Array("100", "1000", "10000"))
    val nbrOfElements: Int = 0

    @Param(Array("0.01", "0.001"))
    val falsePositiveRate: Double = 0

    var randomStrings: Seq[String] = _

    @Setup(Level.Trial)
    def setup(): Unit =
      randomStrings = createRandomString(nbrOfElements, 10)
  }
}

@State(Scope.Benchmark)
class BloomFilterCreateBenchmark {
  import BloomFilterCreateBenchmark._

  /**
   * Create a bloom filter by aggregating on a monoid.
   * This is the most efficient way to create the bloom filter.
   */
  @Benchmark
  def scioMutableBF(bloomFilterState: BloomFilterState): MutableBF[String] = {
    val bfMonoid =
      BloomFilter[String](bloomFilterState.nbrOfElements, bloomFilterState.falsePositiveRate)
    val bfAggregator = BloomFilterAggregator(bfMonoid)

    val sBf = bloomFilterState.randomStrings.aggregate(bfAggregator.monoid.zero)(_ += _, _ ++= _)
    sBf
  }
}
