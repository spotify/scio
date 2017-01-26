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

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.values.SideOutput

/*
SBT
runMain
  com.spotify.scio.examples.extra.SideInOutExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --stopWords=[STOP_WORDS_URI]
  --output1=gs://[BUCKET]/[PATH]/output1
  --output2=gs://[BUCKET]/[PATH]/output2
  --output3=gs://[BUCKET]/[PATH]/output3
  --output4=gs://[BUCKET]/[PATH]/output4
*/

object SideInOutExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val stopWords = args.optional("stopWords") match {
      case Some(path) => sc.textFile(path)
      case None => sc.parallelize(Seq("a", "an", "the", "of", "and", "or"))
    }
    val sideIn = stopWords.map(_ -> Unit).asMapSideInput // SideInput[Map[String, Unit]]
    val wordCount = sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .withSideInputs(sideIn)  // begin side input operation
      .flatMap { (line, ctx) => // (input element, side input context)
      val stop = ctx(sideIn) // extract side input Map[String, Unit]
      line
        .split("[^a-zA-Z']+")
        .filter(_.nonEmpty)
        .map(_.toLowerCase)
        .filter(!stop.contains(_))
    }
      .toSCollection // end side input operation
      .countByValue

    // initialize side outputs
    val oneLetter = SideOutput[(String, Long)]()
    val twoLetter = SideOutput[(String, Long)]()
    val threeLetter = SideOutput[(String, Long)]()

    // (main output, side outputs)
    val (fourOrMoreLetters, sideOutputs) = wordCount
      .withSideOutputs(oneLetter, twoLetter, threeLetter)
      .flatMap { case ((word, count), ctx) =>
        word.length match {
          case 1 => ctx.output(oneLetter, (word, count))
          case 2 => ctx.output(twoLetter, (word, count))
          case 3 => ctx.output(threeLetter, (word, count))
          case _ =>
        }
        if (word.length >= 4) Some((word, count)) else None
      }

    def toString(kv: (String, Long)) = kv._1 + ": " + kv._2

    // save main output
    fourOrMoreLetters.map(toString).saveAsTextFile(args("output4"))

    // extract side output SCollections and save
    sideOutputs(oneLetter).map(toString).saveAsTextFile(args("output1"))
    sideOutputs(twoLetter).map(toString).saveAsTextFile(args("output2"))
    sideOutputs(threeLetter).map(toString).saveAsTextFile(args("output3"))

    sc.close()
  }
}
