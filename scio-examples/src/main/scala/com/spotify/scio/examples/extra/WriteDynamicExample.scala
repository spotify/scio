/*
 * Copyright 2018 Spotify AB.
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

// Example: Demonstrates saveAsDynamic* methods
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.WriteDynamicExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[OUTPUT]"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.io.dynamic._

object WriteDynamicExample {
  case class LinesPerCharacter(name: String, lines: Long)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Compute number of times each character speaks in the King Lear text
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap { line =>
        line.split('\t').toList match {
          case name :: _ :: Nil if name.nonEmpty => Some(name)
          case _                                 => None
        }
      }
      .countByValue
      .map { case (character, count) => s"$character\t$count" }
      // Group records according to first letter of the character's name so
      // all characters starting with letter A will share an output path, etc.
      // Since input is small, restrict to one file per bucket.
      .saveAsDynamicTextFile(args("output"), 1) { l =>
        l.charAt(0).toString.toUpperCase
      }

    sc.close()
  }
}
