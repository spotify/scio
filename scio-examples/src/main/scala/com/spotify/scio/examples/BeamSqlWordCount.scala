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

// Example: Minimal BeamSQL Word Count Example
// Usage:
// `sbt runMain
// "com.spotify.scio.examples.BeamSqlWordCount
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/minimal_wordcount"`
package com.spotify.scio.examples

import com.spotify.scio._
import com.spotify.scio.sql._
import com.spotify.scio.examples.common.ExampleData

object BeamSqlWordCount {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      // Split input lines, filter out empty tokens and expand into a collection of tokens
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // Count distinct string's
      .queryAs[(String, Long)](
        "select distinct(`value`), count(*) from SCOLLECTION group by `value`"
      )
      // Map `(String, Long)` tuples into strings
      .map(t => t._1 + ": " + t._2)
      // Save result as text files under the output path
      .saveAsTextFile(args("output"))

    // Close the context and execute the pipeline
    sc.close()
    ()
  }

}
