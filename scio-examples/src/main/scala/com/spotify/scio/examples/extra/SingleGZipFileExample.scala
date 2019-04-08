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

// Example: Gzip File write
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.SingleGZipFileExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=[INPUT.txt] --output=[OUTPUT]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.io.Compression

object SingleGZipFileExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    // Parse command line arguments, create `ScioContext` and `Args`.
    // `ScioContext` is the entry point to a Scio pipeline. User arguments, e.g.
    // `--input=gs://[BUCKET]/[PATH]/input.txt`, are accessed via `Args`.
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Open text file as `SCollection[String]`. The input can be either a single file or a
    // wildcard matching multiple files.
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      // Split input lines, filter out empty tokens and expand into a collection of tokens
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // Count occurrences of each unique `String` to get `(String, Long)`
      .countByValue
      // Map `(String, Long)` tuples into strings
      .map(t => t._1 + ": " + t._2)

      // Save result as a single text files under the output path, with deflate compression
      .saveAsTextFile(args("output"), numShards = 1, compression = Compression.DEFLATE)

    // Close the context and execute the pipeline
    sc.close()
    ()
  }
}
