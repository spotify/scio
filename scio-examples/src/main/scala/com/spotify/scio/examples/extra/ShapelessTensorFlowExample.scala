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

// Example: Handling TensorFlow Example Types with Shapeless

// TensorFlow `Example` is a Protobuf type and very verbose. By using
// [shapeless-datatype](https://github.com/nevillelyh/shapeless-datatype), one can seamlessly
// convert between case classes and TensorFlow `Example`s.
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.tensorflow._
import org.tensorflow.example.Example
import shapeless.datatype.tensorflow._

object ShapelessTensorFlowExample {
  // Define case class representation of TensorFlow `Example`
  case class WordCount(word: String, count: Long)
  // `TensorFlowType` provides mapping between case classes and TensorFlow `Example`
  val wordCountType = TensorFlowType[WordCount]
}

// ## Shapeless Tensorflow Write Example
// Count words and save result as `TFRecord`s
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.ShapelessTensorFlowWriteExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/wordcount-tf"`
object ShapelessTensorFlowWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessTensorFlowExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      // Convert case class to `Example` and then serialize as `Array[Byte]`
      .map(t => wordCountType.toExample(WordCount.tupled(t)).toByteArray)
      .saveAsTfRecordFile(args("output"))
    sc.run()
    ()
  }
}

// ## Shapeless Tensorflow Read Example
// Read word count result back from `TFRecord`
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.ShapelessTensorFlowReadExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://[BUCKET]/[PATH]/wordcount-tf
// --output=gs://[BUCKET]/[PATH]/wordcount"`
object ShapelessTensorFlowReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessTensorFlowExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.tfRecordFile(args("input"))
      .flatMap { b =>
        // Deserialize `Array[Byte]` as `Example` and then convert to case class
        wordCountType.fromExample(Example.parseFrom(b))
      }
      .map(wc => wc.word + ": " + wc.count)
      .saveAsTextFile(args("output"))
    sc.run()
    ()
  }
}
