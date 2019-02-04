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

// Example: Demonstrates FileIO's writeDynamic method
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.WriteDynamicExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[OUTPUT]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.examples.common.ExampleData
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.FileIO.Write.FileNaming
import org.apache.beam.sdk.io.{FileIO, TextIO}
import org.apache.beam.sdk.transforms.{Contextful, SerializableFunction}

object WriteDynamicExample {
  case class LinesPerCharacter(name: String, lines: Long)

  // The King Lear text is formatted as `$CHARACTER_NAME  $dialogue`
  lazy val kingLearTextSplitter = "\t"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Group records according to first letter of the character's name so
    // all characters starting with letter A will share an output path, etc
    val dynamicOutput: FileIO.Write[String, LinesPerCharacter] = FileIO
      .writeDynamic[String, LinesPerCharacter]()
      .by(new SerializableFunction[LinesPerCharacter, String] {
        override def apply(input: LinesPerCharacter): String =
          input.name.charAt(0).toString.toUpperCase
      })
      .withNaming(
        new SerializableFunction[String, FileNaming] {
          override def apply(firstLetter: String): FileNaming =
            FileIO.Write.defaultNaming(s"characters-starting-with-$firstLetter", ".txt")
        }
      )
      .withDestinationCoder(StringUtf8Coder.of())
      .withNumShards(1) // Since input is small, restrict to one file per bucket
      .via(
        Contextful.fn[LinesPerCharacter, String]( // Output `LinesPerCharacter` records as Strings
          new SerializableFunction[LinesPerCharacter, String] {
            override def apply(input: LinesPerCharacter): String =
              input.toString
          }),
        TextIO.sink()
      )
      .to(args("output"))

    // Compute number of times each character speaks in the King Lear text
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap { line =>
        line.split(kingLearTextSplitter).filter(_.nonEmpty).toList match {
          case name :: dialogue :: Nil => Some(name)
          case _                       => None
        }
      }
      .countByValue
      .map { case (character, count) => LinesPerCharacter(character, count) }
      .saveAsCustomOutput("dynamicWriteExample", dynamicOutput)

    sc.close()
  }
}
