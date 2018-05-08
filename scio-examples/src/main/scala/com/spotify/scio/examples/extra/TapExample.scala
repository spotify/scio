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

// scalastyle:off regex

// Example: Handling Output with Tap and Future
package com.spotify.scio.examples.extra

import com.spotify.scio._

object TapExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Each `ScioContext` instance maps to a unique pipeline

    // First job and its associated `ScioContext`
    val (sc1, args) = ContextAndArgs(cmdlineArgs)
    val f1 = sc1.parallelize(1 to 10)
      .sum
      // Save data to a temporary location for use later as a `Future[Tap[T]]`
      .materialize
    val f2 = sc1.parallelize(1 to 100)
      .sum
      .map(_.toString)
      // Save data for use later as a `Future[Tap[T]]`
      .saveAsTextFile(args("output"))
    sc1.close()

    // Wait for future completions, which should happen when `sc1` finishes
    val t1 = f1.waitForResult()
    val t2 = f2.waitForResult()

    // Fetch `Tap` values directly
    println(t1.value.mkString(", "))
    println(t2.value.mkString(", "))

    // Second job and its associated `ScioContext`
    val (sc2, _) = ContextAndArgs(cmdlineArgs)
    // Re-open taps in new `ScioContext`
    val s = (t1.open(sc2) ++ t2.open(sc2).map(_.toInt)).sum
    // Block until job finishes
    val result = sc2.close().waitUntilFinish()

    println(result.finalState)
  }
}
