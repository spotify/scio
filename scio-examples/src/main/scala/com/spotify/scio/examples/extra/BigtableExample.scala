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

// Example: Bigtable Input and Output
package com.spotify.scio.examples.extra

import com.google.bigtable.v2.{Mutation, Row}
import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.bigtable._
import com.spotify.scio.examples.common.ExampleData
import org.joda.time.Duration

// This depends on APIs from `scio-bigtable` and imports from `com.spotify.scio.bigtable._`.
object BigtableExample {

  val FAMILY_NAME: String = "count"
  val COLUMN_QUALIFIER: ByteString = ByteString.copyFromUtf8("long")

  // Convert a key-value pair to a Bigtable `Mutation` for writing
  def toMutation(key: String, value: Long): (ByteString, Iterable[Mutation]) = {
    val m = Mutations.newSetCell(
      FAMILY_NAME, COLUMN_QUALIFIER, ByteString.copyFromUtf8(value.toString), 0L)
    (ByteString.copyFromUtf8(key), Iterable(m))
  }

  // Convert a Bigtable `Row` from reading to a formatted key-value string
  def fromRow(r: Row): String =
    r.getKey.toStringUtf8 + ": " + r.getValue(FAMILY_NAME, COLUMN_QUALIFIER).get.toStringUtf8

}

// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.BigtableWriteExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --input=gs://apache-beam-samples/shakespeare/kinglear.txt
// --bigtableProjectId=[BIG_TABLE_PROJECT_ID]
// --bigtableInstanceId=[BIG_TABLE_INSTANCE_ID]
// --bigtableTableId=[BIG_TABLE_TABLE_ID]"`

// Count words and save result to Bigtable
object BigtableWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val btProjectId = args("bigtableProjectId")
    val btInstanceId = args("bigtableInstanceId")
    val btTableId = args("bigtableTableId")

    // Bump up the number of bigtable nodes before writing so that the extra traffic does not
    // affect production service. A sleep period is inserted to ensure all new nodes are online
    // before the ingestion starts.
    sc.updateNumberOfBigtableNodes(btProjectId, btInstanceId, 15)

    // Ensure that destination tables and column families exist
    sc.ensureTables(btProjectId, btInstanceId, Map(
      btTableId -> List(BigtableExample.FAMILY_NAME)
    ))

    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(kv => BigtableExample.toMutation(kv._1, kv._2))
      .saveAsBigtable(btProjectId, btInstanceId, btTableId)

    sc.close()

    // Bring down the number of nodes after the job ends to save cost. There is no need to wait
    // after bumping the nodes down.
    sc.updateNumberOfBigtableNodes(btProjectId, btInstanceId, 3, Duration.ZERO)
  }
}

// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.BigtableReadExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --bigtableProjectId=[BIG_TABLE_PROJECT_ID]
// --bigtableInstanceId=[BIG_TABLE_INSTANCE_ID]
// --bigtableTableId=[BIG_TABLE_TABLE_ID]
// --output=gs://[BUCKET]/[PATH]/wordcount"`

// Read word count result back from Bigtable
object BigtableReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val btProjectId = args("bigtableProjectId")
    val btInstanceId = args("bigtableInstanceId")
    val btTableId = args("bigtableTableId")

    sc.bigtable(btProjectId, btInstanceId, btTableId)
      .map(BigtableExample.fromRow)
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
