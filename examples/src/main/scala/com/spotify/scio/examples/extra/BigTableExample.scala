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

import com.google.cloud.bigtable.dataflow.{CloudBigtableScanConfiguration, CloudBigtableTableConfiguration}
import com.spotify.scio._
import com.spotify.scio.bigtable._
import com.spotify.scio.examples.common.ExampleData
import org.apache.hadoop.hbase.client.Put

object BigTableExample {
  val FAMILY = "count".getBytes
  val QUALIFIER = "long".getBytes
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.BigTableWriteExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --bigtableProjectId=[BIG_TABLE_PROJECT_ID]
  --bigtableClusterId=[BIG_TABLE_CLUSTER_ID]
  --bigtableZoneId=[BIG_TABLE_ZONE_ID]
  --bigtableTableId=[BIG_TABLE_TABLE_ID]
*/

object BigTableWriteExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigTableExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val config = CloudBigtableTableConfiguration.fromCBTOptions(BigTable.parseOptions(cmdlineArgs))

    sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .map(kv => new Put(kv._1.getBytes).addColumn(FAMILY, QUALIFIER, kv._2.toString.getBytes))
      .saveAsBigTable(config)

    sc.close()
  }
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.BigTableWriteExample
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --bigtableProjectId=[BIG_TABLE_PROJECT_ID]
  --bigtableClusterId=[BIG_TABLE_CLUSTER_ID]
  --bigtableZoneId=[BIG_TABLE_ZONE_ID]
  --bigtableTableId=[BIG_TABLE_TABLE_ID]
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

object BigTableReadExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import BigTableExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val config = CloudBigtableScanConfiguration.fromCBTOptions(BigTable.parseOptions(cmdlineArgs))

    sc.bigTable(config)
      .map(r => new String(r.getRow) + ": " + new String(r.getValue(FAMILY, QUALIFIER)))
      .saveAsTextFile(args("output"))

    sc.close()
  }
}
