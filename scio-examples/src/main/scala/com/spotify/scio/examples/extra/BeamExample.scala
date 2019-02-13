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

// Example: Mix Beam Java SDK and Scio Code
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.BeamExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --inputTopic=[TOPIC] --outputTopic=[TOPIC]"`
package com.spotify.scio.examples.extra

import java.lang.{Double => JDouble}

import com.spotify.scio._
import com.spotify.scio.avro.Account
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.transforms.{PTransform, Sum}
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.{Pipeline, PipelineResult}
import org.joda.time.Duration

object BeamExample {

  // A Beam native source `PTransform` where the input type is `PBegin`
  def pubsubIn(topic: String): PTransform[PBegin, PCollection[Account]] =
    PubsubIO.readAvros(classOf[Account]).fromTopic(topic)

  // A Beam native windowing `PTransform`
  val window: PTransform[PCollection[Account], PCollection[Account]] =
    Window
      .into[Account](FixedWindows.of(Duration.standardMinutes(60)))
      .triggering(
        AfterWatermark
          .pastEndOfWindow()
          .withEarlyFirings(AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(5)))
          .withLateFirings(AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(10))))
      .accumulatingFiredPanes()

  // A Beam native aggregation `PTransform`
  //
  // `Sum.doublesPerKey()` sums `java.lang.Double` which is a different type from `scala.Double`
  val sumByKey: PTransform[PCollection[KV[String, JDouble]], PCollection[KV[String, JDouble]]] =
    Sum.doublesPerKey[String]()

  // A Beam native sink `PTransform` where the output type is `PDone`
  def pubsubOut(topic: String): PTransform[PCollection[String], PDone] =
    PubsubIO.writeStrings().to(topic)

  def main(cmdlineArgs: Array[String]): Unit = {
    // Parse command line arguments and create Beam specific options plus application specific
    // arguments
    //
    // - opts: `PipelineOptions` or its subtype - Beam pipeline options, where field names and types
    // are defined as setters and getters in the Java interface
    // - args: `Args` - application specific arguments, anything not covered by `opts` ends up here
    val (opts, args) = ScioContext.parseArguments[PipelineOptions](cmdlineArgs)

    // Create a new `ScioContext` with the given `PipelineOptions`
    val sc = ScioContext(opts)

    // Underlying Beam `Pipeline`
    val pipeline: Pipeline = sc.pipeline

    // Custom input with a Beam source `PTransform`
    val accounts: SCollection[Account] =
      sc.customInput("Input", pubsubIn(args("inputTopic")))

    // Underlying Beam `PCollection`
    val p: PCollection[Account] = accounts.internal

    accounts
    // Beam `PTransform`
      .applyTransform(window)
      // Scio `map` transform
      .map(a => KV.of(a.getName.toString, a.getAmount))
      // Beam `PTransform`
      .applyTransform(sumByKey)
      // Scio `map` transform
      .map(kv => kv.getKey + "_" + kv.getValue)
      // Custom output with a Beam sink `PTransform`
      .saveAsCustomOutput("Output", pubsubOut(args("outputTopic")))

    // This calls sc.pipeline.run() under the hood
    val closedContext = sc.close()

    // Underlying Beam pipeline result
    val pipelineResult: PipelineResult = closedContext.pipelineResult
  }

}
