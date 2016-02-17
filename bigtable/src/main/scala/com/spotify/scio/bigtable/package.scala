package com.spotify.scio

import com.google.cloud.bigtable.dataflow.{CloudBigtableIO, CloudBigtableScanConfiguration, CloudBigtableTableConfiguration}
import com.google.cloud.dataflow.sdk.io.Read
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
import org.apache.hadoop.hbase.client.{Mutation, Result, Scan}

import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
 * Main package for BigTable APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.bigtable._
 * }}}
 */
package object bigtable {

  /** Enhanced version of [[ScioContext]] with BigTable methods. */
  // implicit class BigTableScioContext(private val self: ScioContext) extends AnyVal {
  implicit class BigTableScioContext(val self: ScioContext) {

    /** Get an SCollection for a BigTable table. */
    def bigTable(projectId: String,
                 clusterId: String,
                 zoneId: String,
                 tableId: String,
                 scan: Scan = null): SCollection[Result] = self.pipelineOp {
      if (self.isTest) {
        self.getTestInput[Result](BigTableInput(projectId, clusterId, zoneId, tableId))
      } else {
        val _scan: Scan = if (scan != null) scan else new Scan()
        val config = new CloudBigtableScanConfiguration(projectId, zoneId, clusterId, tableId, _scan)
        this.read(config)
      }
    }

    /** Get an SCollection for a BigTable table. */
    def bigTable(config: CloudBigtableScanConfiguration): SCollection[Result] = self.pipelineOp {
      if (self.isTest) {
        self.getTestInput[Result](BigTableInput(config.getProjectId, config.getClusterId, config.getZoneId, config.getTableId))
      } else {
        this.read(config)
      }
    }

    private def read(config: CloudBigtableScanConfiguration): SCollection[Result] =
      self
        .wrap(self.applyInternal(Read.from(CloudBigtableIO.read(config))))
        .setName(s"${config.getProjectId} ${config.getClusterId} ${config.getZoneId} ${config.getTableId}")

  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with BigTable methods. */
  // implicit class BigTableSCollection[T](private val self: SCollection[T]) extends AnyVal {
  implicit class BigTableSCollection[T](val self: SCollection[T]) {

    /** Save this SCollection as a BigTable table. Note that elements must be of type Mutation. */
    def saveAsBigTable(projectId: String,
                       clusterId: String,
                       zoneId: String,
                       tableId: String,
                       additionalConfiguration: Map[String, String] = Map.empty)
                      (implicit ev: T <:< Mutation): Future[Tap[Result]] = {
      if (self.context.isTest) {
        val output = BigTableOutput(projectId, clusterId, zoneId, tableId)
        self.context.testOut(output)(self)
      } else {
        CloudBigtableIO.initializeForWrite(self.context.pipeline)
        val config = new CloudBigtableTableConfiguration(projectId, zoneId, clusterId, tableId, additionalConfiguration.asJava)
        this.write(config)
      }
      Future.failed(new NotImplementedError("BigTable future not implemented"))
    }

    /** Save this SCollection as a BigTable table. Note that elements must be of type Mutation. */
    def saveAsBigTable(config: CloudBigtableTableConfiguration)(implicit ev: T <:< Mutation): Future[Tap[Result]] = {
      if (self.context.isTest) {
        val output = BigTableOutput(config.getProjectId, config.getClusterId, config.getZoneId, config.getTableId)
        self.context.testOut(output)(self)
      } else {
        this.write(config)
      }
      Future.failed(new NotImplementedError("BigTable future not implemented"))
    }

    private def write(config: CloudBigtableTableConfiguration): Unit = {
      CloudBigtableIO.initializeForWrite(self.context.pipeline)
      self.asInstanceOf[SCollection[Mutation]].applyInternal(CloudBigtableIO.writeToTable(config))
    }

  }

  case class BigTableInput(projectId: String, clusterId: String, zoneId: String, tableId: String)
    extends TestIO[Result](s"$projectId\t$clusterId\t$zoneId\t$tableId")

  case class BigTableOutput[T <: Mutation](projectId: String, clusterId: String, zoneId: String, tableId: String)
    extends TestIO[T](s"$projectId\t$clusterId\t$zoneId\t$tableId")

}
