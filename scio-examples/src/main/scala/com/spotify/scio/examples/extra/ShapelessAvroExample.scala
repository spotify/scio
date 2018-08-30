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
import com.spotify.scio.avro._
import com.spotify.scio.examples.common.ExampleData
import org.apache.avro.generic.GenericRecord

/*
 * Avro examples using shapeless-datatype to seamlessly convert between case classes and Avro
 * generic records.
 *
 * https://github.com/nevillelyh/shapeless-datatype
 */
object ShapelessAvroExample {
  // limit import scope to avoid polluting namespace
  import shapeless.datatype.avro._

  val wordCountType = AvroType[WordCount]
  val wordCountSchema = AvroSchema[WordCount]
  case class WordCount(word: String, count: Long)
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.ShapelessAvroWriteExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://apache-beam-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount-avro
*/

// Count words and save result to Avro
object ShapelessAvroWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessAvroExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => wordCountType.toGenericRecord(WordCount.tupled(t)))
      .saveAsAvroFile(args("output"), schema = wordCountSchema)
    sc.close()
  }
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.ShapelessAvroReadExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://[BUCKET]/[PATH]/wordcount-avro
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

// Read word count result back from Avro
object ShapelessAvroReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessAvroExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.avroFile[GenericRecord](args("input"), wordCountSchema)
      .flatMap(e => wordCountType.fromGenericRecord(e))
      .map(wc => wc.word + ": " + wc.count)
      .saveAsTextFile(args("output"))
    sc.close()
  }
}
