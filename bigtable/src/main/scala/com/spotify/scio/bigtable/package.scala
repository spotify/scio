package com.spotify.scio

import com.google.cloud.bigtable.dataflow.{CloudBigtableIO, CloudBigtableScanConfiguration}
import com.google.cloud.dataflow.sdk.io.Read
import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
import org.apache.hadoop.hbase.client.{Mutation, Result, Scan}

import scala.concurrent.Future

package object bigtable {

  // implicit class BigTableScioContext(private val self: ScioContext) extends AnyVal {
  implicit class BigTableScioContext(val self: ScioContext) {

    /** Get an SCollection for a BigTable table. */
    def bigTable(projectId: String, clusterId: String, zoneId: String, tableId: String, scan: Scan = null): SCollection[Result] =
      if (self.isTest) {
        self.getTestInput[Result](BigTableInput(projectId, clusterId, zoneId, tableId))
      } else {
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

  // implicit class BigTableSCollection[T](private val self: SCollection[T]) extends AnyVal {
  implicit class BigTableSCollection[T](val self: SCollection[T]) {

    /** Save this SCollection as a BigTable table. Note that elements must be of type Mutation. */
    def saveAsBigTable(projectId: String,
                       clusterId: String,
                       zoneId: String,
                       tableId: String)(implicit ev: T <:< Mutation): Future[Tap[Result]] = {
      if (self.context.isTest) {
        self.context.testOut(BigTableOutput(projectId, clusterId, zoneId, tableId))(self.internal.asInstanceOf[PCollection[T]])
      } else {
        CloudBigtableIO.initializeForWrite(self.context.pipeline)

        val config = new CloudBigtableScanConfiguration.Builder()
          .withProjectId(projectId)
          .withClusterId(clusterId)
          .withZoneId(zoneId)
          .withTableId(tableId)
          .build()

        self.asInstanceOf[SCollection[Mutation]].applyInternal(CloudBigtableIO.writeToTable(config))
      }
      Future.failed(new NotImplementedError("BigTable future not implemented"))
    }

  }

  case class BigTableInput(projectId: String, clusterId: String, zoneId: String, tableId: String)
    extends TestIO[Result](s"$projectId\t$clusterId\t$zoneId\t$tableId")

  case class BigTableOutput[T <: Mutation](projectId: String, clusterId: String, zoneId: String, tableId: String)
    extends TestIO[T](s"$projectId\t$clusterId\t$zoneId\t$tableId")

}
