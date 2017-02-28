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

import java.lang.{Double => JDouble}
import com.spotify.scio._
import com.spotify.scio.avro.Account
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.coders.{AvroCoder, DoubleCoder, KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.io.PubsubIO
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.transforms.{PTransform, Sum}
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.{Pipeline, PipelineResult}
import org.joda.time.Duration

// Use Beam Java SDK code inside a Scio job
object BeamExample {

  // A Beam native source PTransform
  def pubsubIn(topic: String): PTransform[PBegin, PCollection[Account]] =
    PubsubIO.read[Account]()
      .topic(topic)
      .withCoder(AvroCoder.of(classOf[Account]))

  // A Beam native windowing PTransform
  val window: PTransform[PCollection[Account], PCollection[Account]] =
    Window
      .into[Account](FixedWindows.of(Duration.standardMinutes(60)))
      .triggering(
        AfterWatermark
          .pastEndOfWindow()
          .withEarlyFirings(
            AfterProcessingTime
              .pastFirstElementInPane()
              .plusDelayOf(Duration.standardMinutes(5)))
          .withLateFirings(
            AfterProcessingTime
              .pastFirstElementInPane()
              .plusDelayOf(Duration.standardMinutes(10))))
      .accumulatingFiredPanes()

  // A Beam native aggregation PTransform
  val sumByKey: PTransform[PCollection[KV[String, JDouble]], PCollection[KV[String, JDouble]]] =
    Sum.doublesPerKey[String]()

  // A Beam native sink PTransform
  def pubsubOut(topic: String): PTransform[PCollection[KV[String, JDouble]], PDone] =
    PubsubIO.write[KV[String, JDouble]]()
      .topic(topic)
      .withCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()))

  // scalastyle:off regex
  def main(cmdlineArgs: Array[String]): Unit = {
    // Parse command line arguments and create Beam specific options plus application specific
    // arguments.
    // opts: PipelineOptions - Beam PipelineOptions
    // args: Args - application specific arguments
    val (opts, args) = ScioContext.parseArguments[PipelineOptions](cmdlineArgs)

    val sc = ScioContext.apply(opts)

    // Underlying Beam pipeline
    val pipeline: Pipeline = sc.pipeline
    println(pipeline.getRunner)

    // Apply a Beam source PTransform and get a Scio SCollection
    val accounts: SCollection[Account] = sc.customInput("Input", pubsubIn(args("inputTopic")))

    // Underlying Beam PCollection
    val p: PCollection[Account] = accounts.internal
    println(p.getName)

    accounts
      // Beam PTransform
      .applyTransform(window)
      // Scio transform
      .map(a => KV.of(a.getName.toString, a.getAmount))
      // Beam PTransform
      .applyTransform(sumByKey)
      // Beam sink PTransform[PCollection[T], PDone]
      .saveAsCustomOutput("Output", pubsubOut(args("outputTopic")))

    val result = sc.close()

    // Underlying Beam pipeline result
    val pipelineResult: PipelineResult = result.internal
    println(pipelineResult.getState)
  }
  // scalastyle:on regex

}
