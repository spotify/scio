package com.spotify.scio

import com.google.cloud.bigtable.dataflow.{CloudBigtableIO, CloudBigtableScanConfiguration}
import com.google.cloud.dataflow.sdk.io.Read
import com.spotify.scio.values.SCollection
import org.apache.hadoop.hbase.client.{Mutation, Scan, Result}

package object bigtable {

  implicit class BigTableScioContext(val self: ScioContext) extends AnyVal {
    def bigTable(projectId: String, clusterId: String, zoneId: String, tableId: String, scan: Scan = null): SCollection[Result] = {
      val builder = new CloudBigtableScanConfiguration.Builder()
        .withProjectId(projectId)
        .withClusterId(clusterId)
        .withZoneId(zoneId)
        .withTableId(tableId)
      if (scan != null) {
        builder.withScan(scan)
      }

      self.wrap(self.applyInternal(Read.from(CloudBigtableIO.read(builder.build()))))
    }
  }

  implicit class BigTableSCollection[T](val self: SCollection[T]) extends AnyVal {
    def saveAsBigTable(projectId: String, clusterId: String, zoneId: String, tableId: String)(implicit ev: T <:< Mutation): Unit = {
      CloudBigtableIO.initializeForWrite(self.context.pipeline)

      val config = new CloudBigtableScanConfiguration.Builder()
        .withProjectId(projectId)
        .withClusterId(clusterId)
        .withZoneId(zoneId)
        .withTableId(tableId)
        .build()

      self.asInstanceOf[SCollection[Mutation]].applyInternal(CloudBigtableIO.writeToTable(config))
    }
  }

}
