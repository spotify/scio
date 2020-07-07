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

// Example: Minimal Word Count Examples with typed arguments
package com.spotify.scio.examples

import caseapp._
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.options.{Default, PipelineOptions}
import org.apache.beam.sdk.options.Validation.Required

// Usage:
// `sbt "runMain
// com.spotify.scio.examples.MinimalWordCounCaseAppExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/minimal_wordcount"`
object MinimalWordCounCaseAppExample {
  @AppName("Scio Examples")
  @AppVersion(BuildInfo.version)
  @ProgName("com.spotify.scio.examples.MinimalWordCount")
  case class Arguments(
    @HelpMessage("Path of the file to read from")
    @ExtraName("i")
    input: String = ExampleData.KING_LEAR,
    @HelpMessage("Path of the file to write to")
    @ExtraName("o")
    output: String
  )

  def main(cmdlineArgs: Array[String]): Unit = {
    // Parse command line arguments, create `ScioContext` and `Args`.
    // `ScioContext` is the entry point to a Scio pipeline. User arguments, e.g.
    // `--input=gs://[BUCKET]/[PATH]/input.txt`, are accessed via `Args`.
    val (sc, args) = ContextAndArgs.typed[Arguments](cmdlineArgs)

    // Open text file as `SCollection[String]`. The input can be either a single file or a
    // wildcard matching multiple files.
    sc.textFile(args.input)
      .transform("counter") {
        // Split input lines, filter out empty tokens and expand into a collection of tokens
        _.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
        // Count occurrences of each unique `String` to get `(String, Long)`
          .countByValue
      }
      // Map `(String, Long)` tuples into strings
      .map(t => t._1 + ": " + t._2)
      // Save result as text files under the output path
      .saveAsTextFile(args.output)

    // Execute the pipeline
    sc.run()
    ()
  }
}

// Usage:
// `sbt "runMain
// com.spotify.scio.examples.MinimalWordCounPipelineOptionsExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/minimal_wordcount"`
object MinimalWordCounPipelineOptionsExample {
  trait Arguments extends PipelineOptions {
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    def getInput: String

    def setInput(input: String): Unit

    @Required
    def getOutput: String

    def setOutput(output: String): Unit
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    // Parse command line arguments, create `ScioContext` and `Args`.
    // `ScioContext` is the entry point to a Scio pipeline. User arguments, e.g.
    // `--input=gs://[BUCKET]/[PATH]/input.txt`, are accessed via `Args`.
    val (sc, args) = ContextAndArgs.typed[Arguments](cmdlineArgs)

    // Open text file as `SCollection[String]`. The input can be either a single file or a
    // wildcard matching multiple files.
    sc.textFile(args.getInput)
      .transform("counter") {
        // Split input lines, filter out empty tokens and expand into a collection of tokens
        _.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
        // Count occurrences of each unique `String` to get `(String, Long)`
          .countByValue
      }
      // Map `(String, Long)` tuples into strings
      .map(t => t._1 + ": " + t._2)
      // Save result as text files under the output path
      .saveAsTextFile(args.getOutput)

    // Execute the pipeline
    sc.run()
    ()
  }
}
