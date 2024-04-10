/*
 * Copyright 2020 Spotify AB.
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

// Example: Handling Bigtable Mutations and Rows with Magnolify

// Bigtable `Mutation` and `Row` classes are Bigtable low level API tightly
// coupled types. Writing/reading a pair of values to/from a Bigtable table
// requires to produce redundant non-reusable and error-prone code. By using
// [Magnolify](https://github.com/spotify/magnolify), one can seamlessly
// convert a case classes to `Seq[Mutation]` for writing and a `Row` to
// a case class when reading data back from a Bigtable table.
package com.spotify.scio.examples.extra

import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.bigtable._
import com.spotify.scio.examples.common.ExampleData
import magnolify.bigtable._

import scala.collection.compat._

object MagnolifyBigtableExample {
  case class WordCount(cnt: Long)
  val ColumnFamily = "counts"
}

// ## Magnolify Bigtable Write Example
// Count words and save result to a Bigtable table

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.MagnolifyBigtableWriteExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --bigtableProjectId=[BIG_TABLE_PROJECT_ID]
// --bigtableInstanceId=[BIG_TABLE_INSTANCE_ID]
// --bigtableTableId=[BIG_TABLE_TABLE_ID]
object MagnolifyBigtableWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import MagnolifyBigtableExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val btProjectId = args("bigtableProjectId")
    val btInstanceId = args("bigtableInstanceId")
    val btTableId = args("bigtableTableId")

    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .mapValues(cnt => WordCount(cnt))
      // `keyFn` converts word to a ByteString, while the value is converted via an
      // implicitly derived BigtableType[WordCount]
      .saveAsBigtable(btProjectId, btInstanceId, btTableId, ColumnFamily, ByteString.copyFromUtf8 _)

    sc.run()
    ()
  }
}

// ## Magnolify Bigtable Read example
// Read word count result back from Bigtable

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.MagnolifyBigtableReadExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --bigtableProjectId=[BIG_TABLE_PROJECT_ID]
// --bigtableInstanceId=[BIG_TABLE_INSTANCE_ID]
// --bigtableTableId=[BIG_TABLE_TABLE_ID]
// --output=gs://[BUCKET]/[PATH]/wordcount"`
object MagnolifyBigtableReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import MagnolifyBigtableExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val btProjectId = args("bigtableProjectId")
    val btInstanceId = args("bigtableInstanceId")
    val btTableId = args("bigtableTableId")

    // Internally converts Bigtable `Row` to `(String, WordCount)` via implicit
    // BigtableType[WordCount] and the provided `keyFn`
    sc.typedBigtable[String, WordCount](
      btProjectId,
      btInstanceId,
      btTableId,
      ColumnFamily,
      (bs: ByteString) => bs.toStringUtf8
    ).saveAsTextFile(args("output"))

    sc.run()
    ()
  }
}
