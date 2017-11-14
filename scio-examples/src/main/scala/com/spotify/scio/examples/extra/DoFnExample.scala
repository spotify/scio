/*
 * Copyright 2017 Spotify AB.
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

// Example: DoFn Example
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.DoFnExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/do_fn_example"`
package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}

object DoFnExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // Apply a native Java Beam SDK `Transform[PCollection[InputT], PCollection[OutputT]]`, in
      // this case a `ParDo` of `DoFn[InputT, OutputT]`
      .applyTransform(ParDo.of(new DoFn[String, Int]{
        // `private[<package>]` is required for `processElement` method to be publicly visible in
        // compiled class file, which is a requirement for `@ProcessElement` annotated method
        @ProcessElement
        private[extra] def processElement(c: DoFn[String, Int]#ProcessContext): Unit = {
          c.output(c.element().length)
        }
      }))
      .saveAsTextFile(args("output"))
    sc.close()
  }
}
