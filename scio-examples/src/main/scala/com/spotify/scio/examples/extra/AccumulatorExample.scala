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
import com.spotify.scio.accumulators._
import com.spotify.scio.values.Accumulator

// Update accumulators inside a job and retrieve values later
object AccumulatorExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val sc = ScioContext()

    // create accumulators to be updated inside the pipeline
    val max = sc.maxAccumulator[Int]("max")
    val min = sc.minAccumulator[Int]("min")
    val sum = sc.sumAccumulator[Int]("sum")
    val count = sc.sumAccumulator[Int]("count")

    sc.parallelize(1 to 100)
      .withAccumulator(max, min, sum, count)  // accumulators to be used in the next transform
      .filter { (i, c) =>  // accumulators available via the second argument AccumulatorContext
        // update accumulators via the context
        c.addValue(max, i).addValue(min, i).addValue(sum, i).addValue(count, 1)
        i <= 50
      }
      .map { (i, c) =>
        // reuse some accumulators here
        c.addValue(sum, i).addValue(count, 1)
        i
      }

    val r = sc.close()

    // access accumulator values after job is submitted
    // scalastyle:off regex
    println("Max: " + r.accumulatorTotalValue(max))
    println("Min: " + r.accumulatorTotalValue(min))
    println("Sum: " + r.accumulatorTotalValue(sum))
    println("Count: " + r.accumulatorTotalValue(count))

    println("Sum per step:")
    r.accumulatorValuesAtSteps(sum).foreach(kv => println(kv._2 + " @ " + kv._1))

    println("Count per step:")
    r.accumulatorValuesAtSteps(count).foreach(kv => println(kv._2 + " @ " + kv._1))
    // scalastyle:on regex
  }
}

// Simplified version using helpers from com.spotify.scio.accumulators._
object SimpleAccumulatorExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val sc = ScioContext()

    // create accumulators to be updated inside the pipeline
    val max = sc.maxAccumulator[Int]("max")
    val min = sc.minAccumulator[Int]("min")
    val sum = sc.sumAccumulator[Int]("sum")

    sc.parallelize(1 to 100)
      .accumulate(max, min, sum)
      .accumulateCount
      .accumulateCountFilter(_ <= 50)
      .accumulate(sum)
      .accumulateCount

    val r = sc.close()

    // access accumulator values after job is submitted
    // scalastyle:off regex
    println("Max: " + r.accumulatorTotalValue(max))
    println("Min: " + r.accumulatorTotalValue(min))
    println("Sum: " + r.accumulatorTotalValue(sum))

    println("Sum per step:")
    r.accumulatorValuesAtSteps(sum).foreach(kv => println(kv._2 + " @ " + kv._1))

    println("Count:")
    r.accumulators.filter(_.name.startsWith("accumulateCount")).foreach { a =>
      val v = r.accumulatorTotalValue(a.asInstanceOf[Accumulator[Long]])
      println(v + " @ " + a.name)
    }

    println("Filter:")
    r.accumulators.filter(_.name.contains("accumulateCountFilter")).foreach { a =>
      val v = r.accumulatorTotalValue(a.asInstanceOf[Accumulator[Long]])
      println(v + " @ " + a.name)
    }
    // scalastyle:on regex
  }
}
