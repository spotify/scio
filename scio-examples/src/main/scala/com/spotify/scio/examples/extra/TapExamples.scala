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

// Example: Handling I/O with Tap and Future
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.io.Taps

// ## Tap Output example
// Save collections as text files, then open their `Tap`s in a new `ScioContext`

// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.TapOutExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --output=gs://[OUTPUT] --method=[METHOD]"`
object TapOutputExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Each `ScioContext` instance maps to a unique pipeline

    // First job and its associated `ScioContext`
    val (sc1, args) = ContextAndArgs(cmdlineArgs)
    val f1 = sc1
      .parallelize(1 to 10)
      .sum
      // Save data to a temporary location for use later as a `Future[Tap[T]]`
      .materialize
    val f2 = sc1
      .parallelize(1 to 100)
      .sum
      .map(_.toString)
      // Save data for use later as a `Future[Tap[T]]`
      .saveAsTextFile(args("output"))
    sc1.close()

    // Wait for `Future` completions, which should happen when `sc1` finishes
    val t1 = f1.waitForResult()
    val t2 = f2.waitForResult()

    // scalastyle:off regex
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
    // scalastyle:on regex
  }
}

// ## Tap Input example
// Use `Tap`s as input to defer execution logic until both `Future`s complete

// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.TapInputExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]"`
object TapInputExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val taps = Taps() // entry point to acquire taps

    // extract `Tap[T]`s from two `Future[Tap[T]]`s
    val r = for {
      t1 <- taps.textFile(ExampleData.KING_LEAR)
      t2 <- taps.textFile(ExampleData.OTHELLO)
    } yield {
      // execution logic when both taps are available
      val (sc, _) = ContextAndArgs(cmdlineArgs)
      val out = (t1.open(sc) ++ t2.open(sc))
        .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
        .countByValue
        .map(kv => kv._1 + "\t" + kv._2)
        .materialize
      sc.close()
      out
    }

    // scalastyle:off regex
    println(r.waitForResult().value.take(10).toList)
    // scalastyle:on regex
  }
}
