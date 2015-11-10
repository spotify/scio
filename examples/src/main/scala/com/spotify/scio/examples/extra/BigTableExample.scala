package com.spotify.scio.examples.extra

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
  --bigTableProjectId=[BIG_TABLE_PROJECT_ID]
  --bigTableClusterId=[BIG_TABLE_CLUSTER_ID]
  --bigTableZoneId=[BIG_TABLE_ZONE_ID]
  --bigTableTableId=[BIG_TABLE_TABLE_ID]
*/

object BigTableWriteExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    import BigTableExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt")
    sc.textFile(input)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .map(kv => new Put(kv._1.getBytes).addColumn(FAMILY, QUALIFIER, kv._2.toString.getBytes))
      .saveAsBigTable(
        args("bigTableProjectId"),
        args("bigTableClusterId"),
        args("bigTableZoneId"),
        args("bigTableTableId"))

    sc.close()
  }

}

object BigTableReadExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    import BigTableExample._

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.bigTable(
      args("bigTableProjectId"),
      args("bigTableClusterId"),
      args("bigTableZoneId"),
      args("bigTableTableId"))
      .map(r => new String(r.getRow) + ": " + new String(r.getValue(FAMILY, QUALIFIER)))
      .saveAsTextFile(args("output"))

    sc.close()
  }

}
