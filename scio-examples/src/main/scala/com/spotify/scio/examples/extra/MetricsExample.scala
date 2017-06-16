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

// Update metrics inside a job and retrieve values later
object MetricsExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // create accumulators to be updated inside the pipeline
    val dist = ScioMetrics.distribution("dist")  // This will track min, max, count, sum, mean

    sc.parallelize(1 to 100)
      .filter { i =>
      // update metrics
        dist.update(i)
        i <= 50
     }
      .map { i =>
        // reuse metrics here
        dist.update(i)
        i
      }

    val result = sc.close().waitUntilFinish()

    // access metric values after job is submitted
    // scalastyle:off regex
    val d = result.distribution(dist).committed.get
    println("Min: " + d.min())
    println("Max: " + d.max())
    println("Count: " + d.count())
    println("Sum: " + d.sum())
    println("Mean: " + d.mean())
    // scalastyle:on regex
  }
}
