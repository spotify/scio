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

import com.google.cloud.bigtable.dataflow.{CloudBigtableIO, CloudBigtableScanConfiguration, CloudBigtableTableConfiguration}
import com.google.cloud.dataflow.sdk.io.Read
import com.google.cloud.dataflow.sdk.transforms.PTransform
import com.google.cloud.dataflow.sdk.values.{KV, PDone, PCollection}
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
      val config = new CloudBigtableTableConfiguration(
        projectId, zoneId, clusterId, tableId, additionalConfiguration.asJava)
      this.saveAsBigTable(config)
    }

    /** Save this SCollection as a BigTable table. Note that elements must be of type Mutation. */
    def saveAsBigTable(config: CloudBigtableTableConfiguration)(implicit ev: T <:< Mutation): Future[Tap[Result]] = {
      if (self.context.isTest) {
        val output = BigTableOutput(config.getProjectId, config.getClusterId, config.getZoneId, config.getTableId)
        self.context.testOut(output)(self)
      } else {
        CloudBigtableIO.initializeForWrite(self.context.pipeline)
        val transform = CloudBigtableIO.writeToTable(config)
        self.asInstanceOf[SCollection[Mutation]].applyInternal(transform)
      }
      Future.failed(new NotImplementedError("BigTable future not implemented"))
    }

  }

  /**
    * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] for writing to
    * multiple BigTable tables.
    *
    * Keys are table IDs and values are collections of Mutations.
    */
  // implicit class PairBigTableSCollection[K, V](private val self: SCollection[(K, Iterable[V])]) extends AnyVal {
  implicit class PairBigTableSCollection[T](val self: SCollection[(String, Iterable[T])]) {

    /** Save this SCollection as multiple BigTable tables. Note that value elements must be of type Mutation. */
    def saveAsMultipleBigTable(projectId: String,
                               clusterId: String,
                               zoneId: String,
                               additionalConfiguration: Map[String, String] = Map.empty)
                              (implicit ev: T <:< Mutation): Future[Tap[(String, Iterable[Result])]] = {
      val config = new CloudBigtableTableConfiguration(
        projectId, zoneId, clusterId, null, additionalConfiguration.asJava)
      this.saveAsMultipleBigTable(config)
    }

    /** Save this SCollection as multiple BigTable tables. Note that value elements must be of type Mutation. */
    def saveAsMultipleBigTable(config: CloudBigtableTableConfiguration)(implicit ev: T <:< Mutation): Future[Tap[(String, Iterable[Result])]] = {
      if (self.context.isTest) {
        val output = MultipleBigTableOutput(config.getProjectId, config.getClusterId, config.getZoneId)
        self.context.testOut(output.asInstanceOf[TestIO[(String, Iterable[T])]])(self)
      } else {
        CloudBigtableIO.writeToMultipleTables(config)
        val transform = CloudBigtableIO.writeToMultipleTables(config)
        self
          .map(kv => KV.of(kv._1, kv._2.asJava.asInstanceOf[java.lang.Iterable[Mutation]]))
          .applyInternal(transform)
      }
      Future.failed(new NotImplementedError("BigTable future not implemented"))
    }
  }

  case class BigTableInput(projectId: String, clusterId: String, zoneId: String, tableId: String)
    extends TestIO[Result](s"$projectId\t$clusterId\t$zoneId\t$tableId")

  case class BigTableOutput[T <: Mutation](projectId: String, clusterId: String, zoneId: String, tableId: String)
    extends TestIO[T](s"$projectId\t$clusterId\t$zoneId\t$tableId")

  case class MultipleBigTableOutput[T <: Mutation](projectId: String, clusterId: String, zoneId: String)
    extends TestIO[(String, Iterable[T])](s"$projectId\t$clusterId\t$zoneId")

}
