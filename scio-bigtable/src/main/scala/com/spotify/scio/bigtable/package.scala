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

package com.spotify.scio

import com.google.cloud.bigtable.dataflow.{
  CloudBigtableIO, CloudBigtableScanConfiguration, CloudBigtableTableConfiguration
}
import com.google.cloud.dataflow.sdk.io.Read
import com.google.cloud.dataflow.sdk.values.KV
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
import org.apache.hadoop.hbase.client.{Mutation, Result, Scan}

import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
 * Main package for Bigtable APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.bigtable._
 * }}}
 */
package object bigtable {

  /** Enhanced version of [[ScioContext]] with Bigtable methods. */
  // implicit class BigtableScioContext(private val self: ScioContext) extends AnyVal {
  implicit class BigtableScioContext(val self: ScioContext) {

    /** Get an SCollection for a Bigtable table. */
    def bigTable(projectId: String,
                 clusterId: String,
                 zoneId: String,
                 tableId: String,
                 scan: Scan = null): SCollection[Result] = self.pipelineOp {
      val _scan: Scan = if (scan != null) scan else new Scan()
      val config = new CloudBigtableScanConfiguration(projectId, zoneId, clusterId, tableId, _scan)
      this.bigTable(config)
    }

    /** Get an SCollection for a Bigtable table. */
    def bigTable(config: CloudBigtableScanConfiguration): SCollection[Result] = self.pipelineOp {
      if (self.isTest) {
        val input = BigtableInput(
          config.getProjectId, config.getClusterId, config.getZoneId, config.getTableId)
        self.getTestInput[Result](input)
      } else {
        self
          .wrap(self.applyInternal(Read.from(CloudBigtableIO.read(config))))
          .setName(
            s"${config.getProjectId} ${config.getClusterId} " +
            s"${config.getZoneId} ${config.getTableId}")
      }
    }

  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Bigtable methods.
   */
  // implicit class BigtableSCollection[T](private val self: SCollection[T]) extends AnyVal {
  implicit class BigtableSCollection[T](val self: SCollection[T]) {

    /** Save this SCollection as a Bigtable table. Note that elements must be of type Mutation. */
    def saveAsBigtable(projectId: String,
                       clusterId: String,
                       zoneId: String,
                       tableId: String,
                       additionalConfiguration: Map[String, String] = Map.empty)
                      (implicit ev: T <:< Mutation): Future[Tap[Result]] = {
      val config = new CloudBigtableTableConfiguration(
        projectId, zoneId, clusterId, tableId, additionalConfiguration.asJava)
      this.saveAsBigtable(config)
    }

    /** Save this SCollection as a Bigtable table. Note that elements must be of type Mutation. */
    def saveAsBigtable(config: CloudBigtableTableConfiguration)
                      (implicit ev: T <:< Mutation): Future[Tap[Result]] = {
      if (self.context.isTest) {
        val output = BigtableOutput(
          config.getProjectId, config.getClusterId, config.getZoneId, config.getTableId)
        self.context.testOut(output)(self)
      } else {
        CloudBigtableIO.initializeForWrite(self.context.pipeline)
        val sink = CloudBigtableIO.writeToTable(config)
        self.asInstanceOf[SCollection[Mutation]].applyInternal(sink)
      }
      Future.failed(new NotImplementedError("Bigtable future not implemented"))
    }

  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] for writing to
   * multiple Bigtable tables.
   *
   * Keys are table IDs and values are collections of Mutations.
   */
  // implicit class PairBigtableSCollection[T](private val self: SCollection[(String, Iterable[T])])
  //   extends AnyVal {
  implicit class PairBigtableSCollection[T](val self: SCollection[(String, Iterable[T])]) {

    /**
     * Save this SCollection as multiple Bigtable tables. Note that value elements must be of type
     * Mutation.
     */
    def saveAsMultipleBigtable(projectId: String,
                               clusterId: String,
                               zoneId: String,
                               additionalConfiguration: Map[String, String] = Map.empty)
                              (implicit ev: T <:< Mutation)
    : Future[Tap[(String, Iterable[Result])]] = {
      val config = new CloudBigtableTableConfiguration(
        projectId, zoneId, clusterId, null, additionalConfiguration.asJava)
      this.saveAsMultipleBigtable(config)
    }

    /**
     * Save this SCollection as multiple Bigtable tables. Note that value elements must be of type
     * Mutation.
     */
    def saveAsMultipleBigtable(config: CloudBigtableTableConfiguration)
                              (implicit ev: T <:< Mutation)
    : Future[Tap[(String, Iterable[Result])]] = {
      if (self.context.isTest) {
        val output = MultipleBigtableOutput(
          config.getProjectId, config.getClusterId, config.getZoneId)
        self.context.testOut(output.asInstanceOf[TestIO[(String, Iterable[T])]])(self)
      } else {
        CloudBigtableIO.writeToMultipleTables(config)
        val transform = CloudBigtableIO.writeToMultipleTables(config)
        self
          .map(kv => KV.of(kv._1, kv._2.asJava.asInstanceOf[java.lang.Iterable[Mutation]]))
          .applyInternal(transform)
      }
      Future.failed(new NotImplementedError("Bigtable future not implemented"))
    }
  }

  case class BigtableInput(projectId: String, clusterId: String, zoneId: String, tableId: String)
    extends TestIO[Result](s"$projectId\t$clusterId\t$zoneId\t$tableId")

  case class BigtableOutput[T <: Mutation](projectId: String,
                                           clusterId: String,
                                           zoneId: String,
                                           tableId: String)
    extends TestIO[T](s"$projectId\t$clusterId\t$zoneId\t$tableId")

  case class MultipleBigtableOutput[T <: Mutation](projectId: String,
                                                   clusterId: String,
                                                   zoneId: String)
    extends TestIO[(String, Iterable[T])](s"$projectId\t$clusterId\t$zoneId")

}
