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

package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.tensorflow._
import org.tensorflow.example.Example
import shapeless.datatype.tensorflow._

/*
 * TensorFlow examples using shapeless-datatype to seamlessly convert between case classes and
 * TensorFlow Example ProtoBuf.
 *
 * https://github.com/nevillelyh/shapeless-datatype
 */
object ShapelessTensorFlowExample {
  val wordCountType = TensorFlowType[WordCount]
  case class WordCount(word: String, count: Long)
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.ShapelessTensorFlowWriteExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount-tf
*/

// Count words and save result to TFRecord
object ShapelessTensorFlowWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessTensorFlowExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args("input"))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => wordCountType.toExample(WordCount.tupled(t)).toByteArray)
      .saveAsTfRecordFile(args("output"))
    sc.close()
  }
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.ShapelessTensorFlowReadExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://[BUCKET]/[PATH]/wordcount-tf
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

// Read word count result back from TFRecord
object ShapelessTensorFlowReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessTensorFlowExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.tfRecordFile(args("input"))
      .flatMap { b =>
        wordCountType.fromExample(Example.parseFrom(b))
      }
      .map(wc => wc.word + ": " + wc.count)
      .saveAsTextFile(args("output"))
    sc.close()
  }
}
