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

// Example: TensorFlow examples using shapeless-datatype
// Shapeless seamlessly converts between case classes and TensorFlow Example ProtoBufs.

// See: https://github.com/nevillelyh/shapeless-datatype shapeless-datatype

// Usage:
// `sbt runMain "com.spotify.scio.examples.extra.TFExampleExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=gs://[BUCKET]/[PATH]/tf-example-features
// --feature-desc-path=gs://[BUCKET]/[PATH]/tf-example-features/_features"`
package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.tensorflow._
import shapeless.datatype.tensorflow._

object WordCountFeatureSpec {
  val featuresType: TensorFlowType[WordCountFeatures] =
    TensorFlowType[WordCountFeatures]
  case class WordCountFeatures(wordLength: Float, count: Float)
}

object TFExampleExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    import WordCountFeatureSpec._

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map { case (s, c) => WordCountFeatures(s.length.toFloat, c.toFloat) }
      .map(featuresType.toExample(_))
      .saveAsTfExampleFile(args("output"))
    sc.close()
  }
}
