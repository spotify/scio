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

// scalastyle:off method.length

// Example: Side Input and Output Example
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.SideInOutExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --stopWords=[STOP_WORDS_URI]
// --output1=gs://[BUCKET]/[PATH]/output1
// --output2=gs://[BUCKET]/[PATH]/output2
// --output3=gs://[BUCKET]/[PATH]/output3
// --output4=gs://[BUCKET]/[PATH]/output4"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.values.SideOutput

object SideInOutExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Prepare an `SCollection[String]` of stop words
    val stopWords = args.optional("stopWords") match {
      case Some(path) => sc.textFile(path)
      case None => sc.parallelize(Seq("a", "an", "the", "of", "and", "or"))
    }

    // Convert stop words to a `SideInput[Map[String, Unit]]`
    val sideIn = stopWords.map(_ -> Unit).asMapSideInput

    // Open text files a `SCollection[String]`
    val wordCount = sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      // Begin side input operation. Any side inputs to be accessed in the following transforms
      // must be specified.
      .withSideInputs(sideIn)
      // Specialized version of `map` with access to side inputs via `SideInputContext`
      .flatMap { (line, ctx) =>
        // Retrieve side input value (`Map[String, Unit]`)
        val stop = ctx(sideIn)
        line
          .split("[^a-zA-Z']+")
          .filter(_.nonEmpty)
          .map(_.toLowerCase)
          // Filter stop words using the map side input
          .filter(!stop.contains(_))
        }
        // End of side input operation, convert back to regular `SCollection`
        .toSCollection
        .countByValue

    // Initialize side outputs
    val oneLetter = SideOutput[(String, Long)]()
    val twoLetter = SideOutput[(String, Long)]()
    val threeLetter = SideOutput[(String, Long)]()

    val (fourOrMoreLetters, sideOutputs) = wordCount
      // Begin side output operation. Any side outputs to be accessed in the following transforms
      // must be specified.
      .withSideOutputs(oneLetter, twoLetter, threeLetter)
      // Specialized version of `map` with access to side outputs via `SideOutputContext`. Returns
      // a tuple 2 where the first element is the main output and the second element is a
      // `SideOutputCollections` that encapsulates side outputs.
      .flatMap { case ((word, count), ctx) =>
        word.length match {
          // Send to side outputs via `SideOutputContext`
          case 1 => ctx.output(oneLetter, (word, count))
          case 2 => ctx.output(twoLetter, (word, count))
          case 3 => ctx.output(threeLetter, (word, count))
          case _ =>
        }
        // Send to main output via return value
        if (word.length >= 4) Some((word, count)) else None
      }

    def toString(kv: (String, Long)) = kv._1 + ": " + kv._2

    // Save main output
    fourOrMoreLetters.map(toString).saveAsTextFile(args("output4"))

    // Extract side outputs from `SideOutputCollections` and save
    sideOutputs(oneLetter).map(toString).saveAsTextFile(args("output1"))
    sideOutputs(twoLetter).map(toString).saveAsTextFile(args("output2"))
    sideOutputs(threeLetter).map(toString).saveAsTextFile(args("output3"))

    // Close the context and execute the pipeline
    sc.close()
  }
}
