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

// Example: Handling Datastore Types with Shapeless

// Official Datastore API uses Protobuf heavily but is very verbose. By using
// [shapeless-datatype](https://github.com/nevillelyh/shapeless-datatype), one can seamlessly
// convert between case classes and Datastore entities.
package com.spotify.scio.examples.extra

import com.google.datastore.v1.client.DatastoreHelper.makeKey
import com.google.datastore.v1.Query
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import shapeless.datatype.datastore._

object ShapelessDatastoreExample {
  val kind = "shapeless"
  // Define case class representation of Datastore entities
  case class WordCount(word: String, count: Long)
  // `DatastoreType` provides mapping between case classes and Datatore entities
  val wordCountType = DatastoreType[WordCount]
}

// ## Shapeless Datastore Write Example
// Count words and save result to Datastore

// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.ShapelessDatastoreWriteExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --output=[PROJECT]"`
object ShapelessDatastoreWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessDatastoreExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map { t =>
        // Convert case class to `Entity.Builder`
        wordCountType
          .toEntityBuilder(WordCount.tupled(t))
          // Set entity key
          .setKey(makeKey(kind, t._1))
          .build()
      }
      .saveAsDatastore(args("output"))
    sc.close()
    ()
  }
}

// ## Shapeless Datastore Read Example
// Read word count result back from Datastore

// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.ShapelessDatastoreReadExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=[PROJECT]
// --output=gs://[BUCKET]/[PATH]/wordcount"`
object ShapelessDatastoreReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import ShapelessDatastoreExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.datastore(args("input"), Query.getDefaultInstance)
      // Convert `Entity` to case class
      .flatMap(e => wordCountType.fromEntity(e))
      .map(wc => wc.word + ": " + wc.count)
      .saveAsTextFile(args("output"))
    sc.close()
    ()
  }
}
