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

import com.spotify.scio.util.{BloomFilter, MutableBF}
import com.twitter.algebird.ApproximateBoolean
import org.openjdk.jmh.annotations._

/**
 * Benchmarks for com.spotify.scio.util.BloomFilter
 *
 * Querying for elements from a BloomFilter.
 */
object BloomFilterQueryBenchmark {
  @State(Scope.Benchmark)
  class BloomFilterState {
    @Param(Array("100", "1000", "10000"))
    val nbrOfElements: Int = 0

    @Param(Array("0.001", "0.01"))
    val falsePositiveRate: Double = 0

    private[scio] var bf: MutableBF[String] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      val randomStrings =
        BloomFilterCreateBenchmark.createRandomString(nbrOfElements, 10)
      bf = BloomFilter[String](nbrOfElements, falsePositiveRate)
        .create(randomStrings: _*)
    }
  }
}

@State(Scope.Benchmark)
class BloomFilterQueryBenchmark {
  import BloomFilterQueryBenchmark._

  @Benchmark
  def scioBloomFilter(bloomFilterState: BloomFilterState): ApproximateBoolean =
    bloomFilterState.bf.contains("1")
}
