/*
 * Copyright 2016 Spotify AB.
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

import java.time.Duration

import com.google.cloud.bigtable.{dataflow => bt}
import com.spotify.scio._
import com.spotify.scio.bigtable._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.values.SCollection
import org.apache.hadoop.hbase.client.{Put, Result}

/*
 * Bigtable examples.
 *
 * This depends on APIs from `scio-bigtable` and imports from `com.spotify.scio.bigtable._`.
 * It also depends on an early release of `com.google.cloud.bigtable:bigtable-hbase-dataflow`
 * and may change in the future.
 */
object BigtableExample {
  val FAMILY = "count".getBytes
  val QUALIFIER = "long".getBytes
  def put(key: String, value: Long): Put =
    new Put(key.getBytes).addColumn(FAMILY, QUALIFIER, value.toString.getBytes)
  def result(r: Result): String =
    new String(r.getRow) + ": " + new String(r.getValue(FAMILY, QUALIFIER))
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.BigtableWriteExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --bigtableProjectId=[BIG_TABLE_PROJECT_ID]
  --bigtableInstanceId=[BIG_TABLE_INSTANCE_ID]
  --bigtableTableId=[BIG_TABLE_TABLE_ID]
*/

// Count words and save result to Bigtable
object BigtableWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val btOptions = Bigtable.parseOptions(cmdlineArgs)
    val config = bt.CloudBigtableTableConfiguration.fromCBTOptions(btOptions )

    // bump up the number of bigtable nodes before writing
    sc.updateNumberOfBigtableNodes(config, 15)

    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(kv => BigtableExample.put(kv._1, kv._2))
      .saveAsBigtable(config)

    sc.close()

    // Bring down the number of nodes after the job ends.
    // There is no need to wait after bumping the nodes down.
    sc.updateNumberOfBigtableNodes(config, 3, Duration.ZERO)
  }
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.BigtableReadExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --bigtableProjectId=[BIG_TABLE_PROJECT_ID]
  --bigtableInstanceId=[BIG_TABLE_INSTANCE_ID]
  --bigtableTableId=[BIG_TABLE_TABLE_ID]
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

// Read word count result back from Bigtable
object BigtableReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val btOptions = Bigtable.parseOptions(cmdlineArgs)
    val config = bt.CloudBigtableScanConfiguration.fromCBTOptions(btOptions)

    // bump up the number of bigtable nodes before writing
    sc.updateNumberOfBigtableNodes(config, 15)

    sc.bigTable(config)
      .map(BigtableExample.result)
      .saveAsTextFile(args("output"))

    sc.close()

    // Bring down the number of nodes after the job ends.
    // There is no need to wait after bumping the nodes down.
    sc.updateNumberOfBigtableNodes(config, 3, Duration.ZERO)
  }
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.MultipleBigtableWriteExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --bigtableProjectId=[BIG_TABLE_PROJECT_ID]
  --bigtableInstanceId=[BIG_TABLE_INSTANCE_ID]
  --bigtableTableId=[BIG_TABLE_TABLE_ID]
*/

// Count words and save result to multiple Bigtable tables
object BigtableMultipleWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val btOptions = Bigtable.parseOptions(cmdlineArgs)
    val config = bt.CloudBigtableTableConfiguration.fromCBTOptions(btOptions)

    def wordCount(name: String, in: SCollection[String]): SCollection[(String, Iterable[Put])] =
      in.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
        .countByValue
        .map(kv => BigtableExample.put(kv._1, kv._2))
        .groupBy(_ => Unit)
        .map(kv => (name, kv._2))

    val kingLear = sc.textFile(args.getOrElse("kinglear", ExampleData.KING_LEAR))
    val othello = sc.textFile(args.getOrElse("othello", ExampleData.OTHELLO))

    (wordCount("kinglear", kingLear) ++ wordCount("othello", othello))
      .saveAsMultipleBigtable(config)

    sc.close()
  }
}
