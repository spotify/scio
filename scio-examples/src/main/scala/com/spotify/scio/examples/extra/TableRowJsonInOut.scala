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

// Example: BigQuery TableRow JSON Input and Output
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.TableRowJsonInOut
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/wikipedia_edits/wiki_data-*.json
// --output=gs://[BUCKET]/[PATH]/wikipedia"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.examples.common.ExampleData

// Read and write BigQuery `TableRow` JSON files
object TableRowJsonInOut {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    // Open text files a `SCollection[TableRow]`
    sc.tableRowJsonFile(args.getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
      .take(100)
      // Save result as text files under the output path
      .saveAsTableRowJsonFile(args("output"))
    sc.close()
  }
}
