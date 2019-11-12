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

// Example: Handling Avro GenericRecord Types with Magnolify

// Using [Magnolify](https://github.com/spotify/magnolify) to seamlessly convert between case
// classes and Avro `GenericRecord`s.
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.coders.Coder
import com.spotify.scio.avro._
import com.spotify.scio.examples.common.ExampleData
import org.apache.avro.generic.GenericRecord

object MagnolifyAvroExample {
  // limit import scope to avoid polluting namespace
  import magnolify.avro._

  val wordCountType = AvroType[WordCount]
  case class WordCount(word: String, count: Long)
}

// ## Magnolify Avro Write Example
// Count words and save result to Avro

// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.MagnolifyAvroWriteExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/wordcount-avro"`
object MagnolifyAvroWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import MagnolifyAvroExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    implicit def genericCoder = Coder.avroGenericRecordCoder(wordCountType.schema)
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => wordCountType(WordCount.tupled(t)))
      .saveAsAvroFile(args("output"), schema = wordCountType.schema)
    sc.run()
    ()
  }
}

// ## Magnolify Avro Read Example
// Read word count result back from Avro

// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.MagnolifyAvroReadExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://[BUCKET]/[PATH]/wordcount-avro
// --output=gs://[BUCKET]/[PATH]/wordcount"`
object MagnolifyAvroReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import MagnolifyAvroExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    implicit def genericCoder = Coder.avroGenericRecordCoder(wordCountType.schema)
    sc.avroFile[GenericRecord](args("input"), wordCountType.schema)
      .map(e => wordCountType(e))
      .map(wc => wc.word + ": " + wc.count)
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}
