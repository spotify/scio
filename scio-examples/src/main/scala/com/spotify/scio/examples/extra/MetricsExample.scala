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

// scalastyle:off method.length
// scalastyle:off regex

// Example: Metrics Example
package com.spotify.scio.examples.extra

import com.spotify.scio._

object MetricsExample {

  // ## Creating metrics

  // Create counters to be incremented inside the pipeline
  val sum = ScioMetrics.counter("sum")
  val sum2 = ScioMetrics.counter("sum2")
  val count = ScioMetrics.counter("count")

  // Distribution to track min, max, count, sum, mean, with optional namespace
  val dist = ScioMetrics.distribution("com.spotify.scio.examples.extra.MetricsExample", "dist")

  // Gauge to track a changing value, with job class as namespace
  val gauge = ScioMetrics.gauge[MetricsExample.type]("gauge")

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Create and initialize counters from ScioContext
    val ctxCount1 = sc.initCounter("ctxcount")
    val ctxCount2 = sc.initCounter("namespace", "ctxcount")

    // ## Accessing metrics
    sc.parallelize(1 to 100)
      .filter { i =>
        // Access metrics inside a lambda function
        sum.inc(i)
        sum2.inc(i)
        count.inc()
        dist.update(i)
        gauge.set(i)
        i <= 50
      }
      .map { i =>
        if (i % 2 == 0) {
          // Create a metric on the fly with dynamic name
          ScioMetrics.counter("even_" + i).inc()
        }
        // Reuse a metric, this will show up as a separate step in the results
        sum2.inc(i)
      }

    val result = sc.close().waitUntilFinish()

    // # Retrieving metrics

    // Access metric values after job is submitted
    val s = result.counter(sum).committed.get
    println("sum: " + s)
    require(s == (1 to 100).sum)

    // `s2` is used in 2 different steps in the pipeline

    // Aggregated value
    val s2 = result.counter(sum2).committed.get
    println("sum2: " + s2)

    // Values at steps
    val s2steps = result.counterAtSteps(sum2).mapValues(_.committed.get)
    s2steps.foreach { case (step, value) =>
      println(s"sum2 at $step: " + value)
    }

    // `s2` should contain 2 steps
    require(s2 == (1 to 100).sum + (1 to 50).sum)
    require(s2steps.values.toSet == Set((1 to 100).sum, (1 to 50).sum))

    val c = result.counter(count).committed.get
    println("count: " + c)
    require(c == 100)

    val g = result.gauge(gauge).committed.get
    println("gauge timestamp: " + g.timestamp())
    println("gauge value: " + g.value())

    val d = result.distribution(dist).committed.get
    println("dist min: " + d.min())
    println("dist max: " + d.max())
    println("dist count: " + d.count())
    println("dist sum: " + d.sum())
    println("dist mean: " + d.mean())
    require(d.min() == 1 && d.max() == 100)
    require(d.count() == 100)
    require(d.sum() == (1 to 100).sum)
    require(d.mean() == (1 to 100).sum / 100.0)

    // Dynamic metrics
    result.allCounters
      .filterKeys(_.name().startsWith("even_"))
      .foreach { case (name, value) =>
        println(name.name() + ": " + value.committed.get)
      }
  }

}
