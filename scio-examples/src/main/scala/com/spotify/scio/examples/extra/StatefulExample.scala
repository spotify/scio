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

// Example: Stateful Processing
// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.StatefulExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]"`
package com.spotify.scio.examples.extra

import java.lang.{Integer => JInt}

import com.spotify.scio._
import org.apache.beam.sdk.state.{StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StateId}
import org.apache.beam.sdk.values.KV

object StatefulExample {
  // States are persisted on a per-key-and-window basis
  type DoFnT = DoFn[KV[String, Int], KV[String, (Int, Int)]]

  class StatefulDoFn extends DoFnT {
    // Declare mutable state
    @StateId("count") private val count = StateSpecs.value[JInt]()

    @ProcessElement
    def processElement(
      context: DoFnT#ProcessContext,
      // Access state declared earlier
      @StateId("count") count: ValueState[JInt]
    ): Unit = {
      // Read and write state
      val c = count.read()
      count.write(c + 1)
      val kv = context.element()
      context.output(KV.of(kv.getKey, (kv.getValue, c)))
    }
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = for {
      k <- Seq("a", "b")
      v <- 1 to 10
    } yield (k, v)

    sc.parallelize(input)
      // Apply a stateful DoFn
      .applyPerKeyDoFn(new StatefulDoFn)
      .debug()
    sc.run()
    ()
  }
}
