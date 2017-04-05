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

import com.google.datastore.v1.Query
import com.spotify.scio._
import shapeless.datatype.datastore._

/*
 * Datastore examples using shapeless-datatype to seamlessly convert between case classes and
 * Datastore entities.
 *
 * https://github.com/nevillelyh/shapeless-datatype
 */
object ShapelessDatastoreExample {
  val wordCountType = DatastoreType[WordCount]
  case class WordCount(word: String, count: Long)
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.ShapelessDatastoreWriteExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=[PROJECT]
*/

// Count words and save result to Datastore
object ShapelessDatastoreWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessDatastoreExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args("input"))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => wordCountType.toEntity(WordCount.tupled(t)))
      .saveAsDatastore(args("output"))
    sc.close()
  }
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.ShapelessDatastoreReadExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=[PROJECT]
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

// Read word count result back from Datastore
object ShapelessDatastoreReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessDatastoreExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.datastore(args("input"), Query.getDefaultInstance)
      .flatMap(e => wordCountType.fromEntity(e))
      .map(wc => wc.word + ": " + wc.count)
      .saveAsTextFile(args("output"))
    sc.close()
  }
}
