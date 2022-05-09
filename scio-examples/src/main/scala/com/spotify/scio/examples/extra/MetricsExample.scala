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

// Example: Metrics Example
// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.MetricsExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import org.apache.beam.sdk.metrics.{Counter, Distribution, Gauge}

import scala.collection.compat._

object MetricsExample {
  // ## Creating metrics

  // Create counters to be incremented inside the pipeline
  val sum: Counter = ScioMetrics.counter("sum")
  val sum2: Counter = ScioMetrics.counter("sum2")
  val count: Counter = ScioMetrics.counter("count")

  // Distribution to track min, max, count, sum, mean, with optional namespace
  val dist: Distribution =
    ScioMetrics.distribution("com.spotify.scio.examples.extra.MetricsExample", "dist")

  // Gauge to track a changing value, with job class as namespace
  val gauge: Gauge = ScioMetrics.gauge[MetricsExample.type]("gauge")

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Create and initialize counters from ScioContext
    sc.initCounter("ctxcount")
    sc.initCounter("namespace", "ctxcount")
    sc.initCounter(count)

    // ## Accessing metrics
    sc.parallelize(1L to 100L)
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

    val result = sc.run().waitUntilFinish()

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
    val s2steps = result.counterAtSteps(sum2).view.mapValues(_.committed.get).toMap
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
    println("gauge timestamp: " + g.getTimestamp)
    println("gauge value: " + g.getValue)

    val d = result.distribution(dist).committed.get
    println("dist min: " + d.getMin)
    println("dist max: " + d.getMax)
    println("dist count: " + d.getCount)
    println("dist sum: " + d.getSum)
    println("dist mean: " + d.getMean)
    require(d.getMin == 1 && d.getMax == 100)
    require(d.getCount == 100)
    require(d.getSum == (1 to 100).sum)
    require(d.getMean == (1 to 100).sum / 100.0)

    // Dynamic metricsk
    result.allCounters.view
      .filterKeys(_.getName.startsWith("even_"))
      .foreach { case (name, value) =>
        println(name.getName + ": " + value.committed.get)
      }
  }
}
