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

package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.metrics.ScioMetric

// Update accumulators inside a job and retrieve values later
object AccumulatorExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // create accumulators to be updated inside the pipeline
    val dist = ScioMetric.distribution("dist")  // This will track min, max, count, sum, mean

    sc.parallelize(1 to 100)
      .filter { i =>
      // update accumulators
        dist.update(i)
        i <= 50
     }
      .map { i =>
        // reuse some accumulators here
        dist.update(i)
        i
      }

    val r = sc.close().waitUntilFinish()

    // access accumulator values after job is submitted
    // scalastyle:off regex
    val resultDist = r.getDistributions()("dist").committed.get
    println("Max: " + resultDist.max)
    println("Min: " + resultDist.min)
    println("Sum: " + resultDist.sum)
    println("Count: " + resultDist.count)
    // scalastyle:on regex
  }
}

// Simplified version using helpers from com.spotify.scio.accumulators._
object SimpleAccumulatorExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // create accumulator to be updated inside the pipeline
    val dist = ScioMetric.distribution("dist")

    sc.parallelize(1 to 100)
      .map(dist.update(_))

    val r = sc.close().waitUntilFinish()

    // access accumulator values after job is submitted
    // scalastyle:off regex
    val stats = r.getDistributions()("dist").committed.get
    println("Max: " + stats.max)
    println("Min: " + stats.min)
    println("Sum: " + stats.sum)
    // scalastyle:on regex
  }
}
