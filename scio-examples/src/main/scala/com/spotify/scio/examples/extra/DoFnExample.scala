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

// Example: Mix Beam DoFn and Scio
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.DoFnExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/do_fn_example"`
package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, ParDo}

object DoFnExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // Apply a native Java Beam SDK `PTransform[PCollection[InputT], PCollection[OutputT]]`, in
      // this case a `ParDo` of `DoFn[InputT, OutputT]`
      //
      // Create one `DoFn` instance in `main`. The instance is serialized, sent to each worker, and
      // de-serialized once per worker thread. By default each worker runs one worker thread per
      // CPU but this can be adjusted with runner specific options, for example
      // `--numberOfWorkerHarnessThreads` in `DataflowPipelineDebugOptions` for `DataflowRunner`.
      //
      // `private[<package>]` is required for anonymous instance methods to be publicly visible
      // in compiled class file, which is a requirement for the annotated methods.
      .applyTransform(ParDo.of(new DoFn[String, Int] {
        // `@Setup` (optional) is called once per worker thread before any processing starts.
        @Setup
        private[extra] def setup(): Unit = ()

        // `@StartBundle` (optional) is called once per worker thread before processing each batch
        // of elements, e.g. elements in a window.
        @StartBundle
        private[extra] def startBundle(c: DoFn[String, Int]#StartBundleContext): Unit = ()

        // `@ProcessElement` is called once per element.
        @ProcessElement
        private[extra] def processElement(c: DoFn[String, Int]#ProcessContext): Unit =
          c.output(c.element().length)

        // `@FinishBundle` (optional) is called once per worker thread after processing each batch
        // of elements, e.g. elements in a window.
        @FinishBundle
        private[extra] def finishBundle(c: DoFn[String, Int]#FinishBundleContext): Unit = ()

        // `@Teardown` (optional) is called once per worker thread after all processing completes.
        @Teardown
        private[extra] def teardown(): Unit = ()
      }))
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}
