package com.spotify.scio.examples.extra

import com.google.cloud.bigtable.dataflow.{CloudBigtableScanConfiguration, CloudBigtableTableConfiguration}
import com.spotify.scio._
import com.spotify.scio.bigtable._
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

    val input = args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt")
    sc.textFile(input)
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
  --output=gs://[BUCKET]/dataflow/wordcount
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
