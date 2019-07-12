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

// Example: Pubsub Example using templates:
// https://cloud.google.com/dataflow/docs/templates/overview

// Usage:

// To upload the template:
// `sbt runMain "com.spotify.scio.examples.extra.TemplateExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --stagingLocation=gs://[BUCKET]/staging --templateLocation=gs://[BUCKET]/TemplateExample"`

// To run the template, e.g. from gcloud:
// `gcloud dataflow jobs run [JOB-NAME] --gcs-location gs://[BUCKET]/TemplateExample \
// --parameters inputSub=projects/[PROJECT]/subscriptions/sub,\
//  outputTopic=projects/[PROJECT]/topics/[TOPIC]`

// To run the job directly:
// `sbt runMain "com.spotify.scio.examples.extra.TemplateExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --inputSub=projects/[PROJECT]/subscriptions/sub
// --outputTopic=projects/[PROJECT]/topics/[TOPIC]"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.options.{
  Description,
  PipelineOptions,
  PipelineOptionsFactory,
  StreamingOptions,
  ValueProvider
}
import org.apache.beam.sdk.options.Validation.Required

object TemplateExample {

  trait Options extends PipelineOptions with StreamingOptions {
    @Description("The Cloud Pub/Sub subscription to read from")
    @Required
    def getInputSubscription: ValueProvider[String]
    def setInputSubscription(value: ValueProvider[String]): Unit

    @Description("The Cloud Pub/Sub topic to write to")
    @Required
    def getOutputTopic: ValueProvider[String]
    def setOutputTopic(value: ValueProvider[String]): Unit
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    PipelineOptionsFactory.register(classOf[Options])
    val options = PipelineOptionsFactory
      .fromArgs(cmdlineArgs: _*)
      .withValidation
      .as(classOf[Options])
    options.setStreaming(true)
    run(options)
  }

  def run(options: Options): Unit = {
    val sc = ScioContext(options)

    val inputIO = PubsubIO.readStrings().fromSubscription(options.getInputSubscription)
    val outputIO = PubsubIO.writeStrings().to(options.getOutputTopic)

    // We have to use custom inputs and outputs to work with `ValueProvider`s
    sc.customInput("input", inputIO)
      .saveAsCustomOutput("output", outputIO)

    // Close the context
    sc.run()
    ()
  }
}
