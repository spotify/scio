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

import com.google.cloud.bigtable.dataflow.CloudBigtableConfiguration
import com.google.cloud.bigtable.{dataflow => bt}
import com.google.cloud.dataflow.sdk.io.Read
import com.google.cloud.dataflow.sdk.values.KV
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
import org.apache.hadoop.hbase.client.{Mutation, Result, Scan}
import org.joda.time.Duration

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

  private val DEFAULT_SLEEP_DURATION = Duration.standardMinutes(20);

  /** Enhanced version of [[ScioContext]] with Bigtable methods. */
  implicit class BigtableScioContext(val self: ScioContext) extends AnyVal {


    /** Get an SCollection for a Bigtable table. */
    def bigTable(projectId: String,
                 instanceId: String,
                 tableId: String,
                 scan: Scan = null): SCollection[Result] = self.requireNotClosed {
      val _scan: Scan = if (scan != null) scan else new Scan()
      val config = new bt.CloudBigtableScanConfiguration.Builder()
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withTableId(tableId)
        .withScan(_scan)
        .build
      this.bigTable(config)
    }

    /** Get an SCollection for a Bigtable table. */
    def bigTable(config: bt.CloudBigtableScanConfiguration): SCollection[Result] =
    self.requireNotClosed {
      if (self.isTest) {
        val input = BigtableInput(
          config.getProjectId, config.getInstanceId, config.getTableId)
        self.getTestInput[Result](input)
      } else {
        self
          .wrap(self.applyInternal(Read.from(bt.CloudBigtableIO.read(config))))
          .setName(s"${config.getProjectId} ${config.getInstanceId} ${config.getTableId}")
      }
    }

    /** Wrapper around update number of Bigtable nodes function to allow for testing. */
    def updateNumberOfBigtableNodes(cloudBigtableConfiguration: CloudBigtableConfiguration,
                                    numberOfNodes: Int,
                                    sleepDuration: Duration = DEFAULT_SLEEP_DURATION): Unit = {
      if (self.isTest) {
        // No need to update the number of nodes in a test
      } else {
        BigtableUtil.updateNumberOfBigtableNodes(
          cloudBigtableConfiguration,
          numberOfNodes,
          sleepDuration
        )
      }
    }

  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Bigtable methods.
   */
  implicit class BigtableSCollection[T](val self: SCollection[T]) extends AnyVal {

    /** Save this SCollection as a Bigtable table. Note that elements must be of type Mutation. */
    def saveAsBigtable(projectId: String,
                       instanceId: String,
                       tableId: String,
                       additionalConfiguration: Map[String, String] = Map.empty)
                      (implicit ev: T <:< Mutation): Future[Tap[Result]] = {
      val config = new bt.CloudBigtableTableConfiguration.Builder()
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withTableId(tableId)
      val configWithConf = additionalConfiguration.foldLeft(config) { case (conf, (key, value)) =>
        conf.withConfiguration(key, value)
      }
      this.saveAsBigtable(configWithConf.build)
    }

    /** Save this SCollection as a Bigtable table. Note that elements must be of type Mutation. */
    def saveAsBigtable(config: bt.CloudBigtableTableConfiguration)
                      (implicit ev: T <:< Mutation): Future[Tap[Result]] = {
      if (self.context.isTest) {
        val output = BigtableOutput(
          config.getProjectId, config.getInstanceId, config.getTableId)
        self.context.testOut(output)(self)
      } else {
        bt.CloudBigtableIO.initializeForWrite(self.context.pipeline)
        val sink = bt.CloudBigtableIO.writeToTable(config)
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
  implicit class PairBigtableSCollection[T](val self: SCollection[(String, Iterable[T])])
    extends AnyVal {

    /**
     * Save this SCollection as multiple Bigtable tables. Note that value elements must be of type
     * Mutation.
     */
    def saveAsMultipleBigtable(projectId: String,
                               instanceId: String,
                               additionalConfiguration: Map[String, String] = Map.empty)
                              (implicit ev: T <:< Mutation)
    : Future[Tap[(String, Iterable[Result])]] = {
      val config = new bt.CloudBigtableTableConfiguration.Builder()
        .withProjectId(projectId)
        .withInstanceId(instanceId)
      val configWithConf = additionalConfiguration.foldLeft(config) { case (conf, (key, value)) =>
        conf.withConfiguration(key, value)
      }
      this.saveAsMultipleBigtable(configWithConf.build)
    }

    /**
     * Save this SCollection as multiple Bigtable tables. Note that value elements must be of type
     * Mutation.
     */
    def saveAsMultipleBigtable(config: bt.CloudBigtableTableConfiguration)
                              (implicit ev: T <:< Mutation)
    : Future[Tap[(String, Iterable[Result])]] = {
      if (self.context.isTest) {
        val output = MultipleBigtableOutput(
          config.getProjectId, config.getInstanceId)
        self.context.testOut(output.asInstanceOf[TestIO[(String, Iterable[T])]])(self)
      } else {
        val transform = BigtableMultiTableWrite.writeToMultipleTables(config)
        self
          .map(kv => KV.of(kv._1, kv._2.asJava.asInstanceOf[java.lang.Iterable[Mutation]]))
          .applyInternal(transform)
      }
      Future.failed(new NotImplementedError("Bigtable future not implemented"))
    }
  }

  case class BigtableInput(projectId: String, instanceId: String, tableId: String)
    extends TestIO[Result](s"$projectId\t$instanceId\t$tableId")

  case class BigtableOutput[T <: Mutation](projectId: String,
                                           instanceId: String,
                                           tableId: String)
    extends TestIO[T](s"$projectId\t$instanceId\t$tableId")

  case class MultipleBigtableOutput[T <: Mutation](projectId: String,
                                                   instanceId: String)
    extends TestIO[(String, Iterable[T])](s"$projectId\t$instanceId")

}
