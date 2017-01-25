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

import java.time.Duration

import com.google.bigtable.v2.{Mutation, Row, RowFilter}
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.protobuf.ByteString
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.apache.beam.sdk.values.KV

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

  private val DEFAULT_SLEEP_DURATION = Duration.ofMinutes(20)

  /** Enhanced version of [[ScioContext]] with Bigtable methods. */
  implicit class BigtableScioContext(val self: ScioContext) extends AnyVal {

    /** Get an SCollection for a Bigtable table. */
    def bigtable(projectId: String,
                 instanceId: String,
                 tableId: String,
                 keyRange: ByteKeyRange = null,
                 rowFilter: RowFilter = null): SCollection[Row] = {
      val bigtableOptions = new BigtableOptions.Builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build
      this.bigtable(bigtableOptions, tableId, keyRange, rowFilter)
    }

    /** Get an SCollection for a Bigtable table. */
    def bigtable(bigtableOptions: BigtableOptions,
                 tableId: String,
                 keyRange: ByteKeyRange,
                 rowFilter: RowFilter): SCollection[Row] =
    self.requireNotClosed {
      if (self.isTest) {
        val input = BigtableInput(
          bigtableOptions.getProjectId,
          bigtableOptions.getInstanceId,
          tableId)
        self.getTestInput[Row](input)
      } else {
        var read = BigtableIO.read()
          .withBigtableOptions(bigtableOptions)
          .withTableId(tableId)
        if (keyRange != null) {
          read = read.withKeyRange(keyRange)
        }
        if (rowFilter != null) {
          read = read.withRowFilter(rowFilter)
        }
        self.wrap(self.applyInternal(read))
          .setName(s"${bigtableOptions.getProjectId} ${bigtableOptions.getInstanceId} $tableId")
      }
    }

    /** Wrapper around update number of Bigtable nodes function to allow for testing. */
    def updateNumberOfBigtableNodes(projectId: String,
                                    instanceId: String,
                                    numberOfNodes: Int,
                                    sleepDuration: Duration = DEFAULT_SLEEP_DURATION): Unit = {
      val bigtableOptions = new BigtableOptions.Builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build
      updateNumberOfBigtableNodes(bigtableOptions, numberOfNodes, sleepDuration)
    }

    /** Wrapper around update number of Bigtable nodes function to allow for testing. */
    def updateNumberOfBigtableNodes(bigtableOptions: BigtableOptions,
                                    numberOfNodes: Int,
                                    sleepDuration: Duration): Unit = {
      // No need to update the number of nodes in a test
      if (!self.isTest) {
        BigtableUtil.updateNumberOfBigtableNodes(
          bigtableOptions,
          numberOfNodes,
          sleepDuration)
      }
    }

  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Bigtable methods.
   */
  implicit class BigtableSCollection[T](val self: SCollection[(ByteString, Iterable[T])])
    extends AnyVal {

    /** Save this SCollection as a Bigtable table. Note that elements must be of type Mutation. */
    def saveAsBigtable(projectId: String,
                       instanceId: String,
                       tableId: String)
                      (implicit ev: T <:< Mutation)
    : Future[Tap[(ByteString, Iterable[Mutation])]] = {
      val bigtableOptions = new BigtableOptions.Builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build
      this.saveAsBigtable(bigtableOptions, tableId)
    }

    /** Save this SCollection as a Bigtable table. Note that elements must be of type Mutation. */
    def saveAsBigtable(bigtableOptions: BigtableOptions,
                       tableId: String)
                      (implicit ev: T <:< Mutation)
    : Future[Tap[(ByteString, Iterable[Mutation])]] = {
      if (self.context.isTest) {
        val output = BigtableOutput(
          bigtableOptions.getProjectId, bigtableOptions.getInstanceId, tableId)
        self.context.testOut(output.asInstanceOf[TestIO[(ByteString, Iterable[T])]])(self)
      } else {
        val sink = BigtableIO.write().withBigtableOptions(bigtableOptions).withTableId(tableId)
        self
          .map(kv => KV.of(kv._1, kv._2.asJava.asInstanceOf[java.lang.Iterable[Mutation]]))
          .applyInternal(sink)
      }
      Future.failed(new NotImplementedError("Bigtable future not implemented"))
    }

  }

  case class BigtableInput(projectId: String, instanceId: String, tableId: String)
    extends TestIO[Row](s"$projectId\t$instanceId\t$tableId")

  case class BigtableOutput[T <: Mutation](projectId: String,
                                           instanceId: String,
                                           tableId: String)
    extends TestIO[(ByteString, Iterable[T])](s"$projectId\t$instanceId\t$tableId")

}
