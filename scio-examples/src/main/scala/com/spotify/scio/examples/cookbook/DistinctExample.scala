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

// Example: Distinct Example
// Usage:

// `sbt runMain "com.spotify.scio.examples.cookbook.DistinctExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
//--output=gs://[BUCKET]/output/path"`
package com.spotify.scio.examples.cookbook

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.examples.common.ExampleData

object DistinctExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val in = args.getOrElse("input", ExampleData.SHAKESPEARE_ALL)
    val out = args("output")

    // Open text files a `SCollection[String]`
    sc.textFile(in)
      // Remove duplicate lines
      .distinct
      // Save result as text files under the output path
      .saveAsTextFile(out)

    // Close the context and execute the pipeline
    sc.close()
  }
}
