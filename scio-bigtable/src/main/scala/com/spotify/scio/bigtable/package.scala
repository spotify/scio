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

import com.google.bigtable.v2._
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.protobuf.ByteString
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.TestIO
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.range.ByteKeyRange
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

  /** Enhanced version of `Row` with convenience methods. */
  implicit class RichRow(val self: Row) extends AnyVal {

    /** Return the `Cell`s for the specific column. */
    def getColumnCells(familyName: String, columnQualifier: ByteString): List[Cell] =
      (for {
        f <- self.getFamiliesList.asScala.find(_.getName == familyName)
        c <- f.getColumnsList.asScala.find(_.getQualifier == columnQualifier)
      } yield c.getCellsList.asScala).toList.flatten

    /** The `Cell` for the most recent timestamp for a given column. */
    def getColumnLatestCell(familyName: String, columnQualifier: ByteString): Option[Cell] =
      getColumnCells(familyName, columnQualifier).headOption

    /** Map of qualifiers to values. */
    def getFamilyMap(familyName: String): Map[ByteString, ByteString] =
      self.getFamiliesList.asScala.find(_.getName == familyName) match {
        case None => Map.empty
        case Some(f) => if (f.getColumnsCount > 0) {
          f.getColumnsList.asScala.map(c => c.getQualifier -> c.getCells(0).getValue).toMap
        } else {
          Map.empty
        }
      }

    /** Map of families to all versions of its qualifiers and values. */
    def getMap: Map[String, Map[ByteString, Map[Long, ByteString]]] = {
      val m = Map.newBuilder[String, Map[ByteString, Map[Long, ByteString]]]
      for (family <- self.getFamiliesList.asScala) {
        val columnMap = Map.newBuilder[ByteString, Map[Long, ByteString]]
        for (column <- family.getColumnsList.asScala) {
          val cellMap = column.getCellsList.asScala
            .map(x => x.getTimestampMicros -> x.getValue)
            .toMap
          columnMap += column.getQualifier -> cellMap
        }
        m += family.getName -> columnMap.result()
      }
      m.result()
    }

    /** Map of families to their most recent qualifiers and values. */
    def getNoVersionMap: Map[String, Map[ByteString, ByteString]] =
      self.getFamiliesList.asScala.map(f => f.getName -> getFamilyMap(f.getName)).toMap

    /** Get the latest version of the specified column. */
    def getValue(familyName: String, columnQualifier: ByteString): Option[ByteString] =
      for {
        f <- self.getFamiliesList.asScala.find(_.getName == familyName)
        c <- f.getColumnsList.asScala.find(_.getQualifier == columnQualifier)
      } yield c.getCells(0).getValue

  }

  private val DEFAULT_SLEEP_DURATION = Duration.standardMinutes(20)

  /** Enhanced version of [[ScioContext]] with Bigtable methods. */
  implicit class BigtableScioContext(val self: ScioContext) extends AnyVal {

    /** Get an SCollection for a Bigtable table. */
    def bigtable(projectId: String,
                 instanceId: String,
                 tableId: String,
                 keyRange: ByteKeyRange = null,
                 rowFilter: RowFilter = null): SCollection[Row] = {
      val parameters = nio.Row.Parameters(keyRange, rowFilter)
      self.read(nio.Row(projectId, instanceId, tableId))(parameters)
    }

    /** Get an SCollection for a Bigtable table. */
    def bigtable(bigtableOptions: BigtableOptions,
                 tableId: String,
                 keyRange: ByteKeyRange,
                 rowFilter: RowFilter): SCollection[Row] = {
      val parameters = nio.Row.Parameters(keyRange, rowFilter)
      self.read(nio.Row(bigtableOptions, tableId))(parameters)
    }

    /**
     * Updates all clusters within the specified Bigtable instance to a specified number of nodes.
     * Useful for increasing the number of nodes at the beginning of a job and decreasing it at
     * the end to lower costs yet still get high throughput during bulk ingests/dumps.
     *
     * @param sleepDuration How long to sleep after updating the number of nodes. Google recommends
     *                      at least 20 minutes before the new nodes are fully functional
     */
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

    /**
     * Updates all clusters within the specified Bigtable instance to a specified number of nodes.
     * Useful for increasing the number of nodes at the beginning of a job and decreasing it at
     * the end to lower costs yet still get high throughput during bulk ingests/dumps.
     *
     * @param sleepDuration How long to sleep after updating the number of nodes. Google recommends
     *                      at least 20 minutes before the new nodes are fully functional
     */
    def updateNumberOfBigtableNodes(bigtableOptions: BigtableOptions,
                                    numberOfNodes: Int,
                                    sleepDuration: Duration): Unit = if (!self.isTest) {
      // No need to update the number of nodes in a test
      BigtableUtil.updateNumberOfBigtableNodes(
        bigtableOptions,
        numberOfNodes,
        sleepDuration)
    }

    /**
     * Get size of all clusters for specified Bigtable instance.
     *
     * @return map of clusterId to its number of nodes
     */
    def getBigtableClusterSizes(projectId: String,
                                instanceId: String): Map[String, Int] = {
      if (!self.isTest) {
        BigtableUtil.getClusterSizes(projectId, instanceId).asScala.toMap.mapValues(_.toInt)
      } else {
        Map.empty
      }
    }

    /**
     * Ensure that tables and column families exist.
     * Checks for existence of tables or creates them if they do not exist.  Also checks for
     * existence of column families within each table and creates them if they do not exist.
     *
     * @param tablesAndColumnFamilies A map of tables and column families.  Keys are table names.
     *                                Values are a list of column family names.
     */
    def ensureTables(projectId: String,
                     instanceId: String,
                     tablesAndColumnFamilies: Map[String, List[String]]): Unit = {
      if (!self.isTest) {
        val bigtableOptions = new BigtableOptions.Builder()
          .setProjectId(projectId)
          .setInstanceId(instanceId)
          .build
        TableAdmin.ensureTables(bigtableOptions, tablesAndColumnFamilies)
      }
    }

    /**
     * Ensure that tables and column families exist.
     * Checks for existence of tables or creates them if they do not exist.  Also checks for
     * existence of column families within each table and creates them if they do not exist.
     *
     * @param tablesAndColumnFamilies A map of tables and column families.  Keys are table names.
     *                                Values are a list of column family names.
     */
    def ensureTables(bigtableOptions: BigtableOptions,
                     tablesAndColumnFamilies: Map[String, List[String]]): Unit = {
      if (!self.isTest) {
        TableAdmin.ensureTables(bigtableOptions, tablesAndColumnFamilies)
      }
    }
  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Bigtable methods.
   */
  implicit class BigtableSCollection[T](val self: SCollection[(ByteString, Iterable[T])])
    extends AnyVal {

    /**
     * Save this SCollection as a Bigtable table. Note that elements must be of type `Mutation`.
     */
    def saveAsBigtable(projectId: String,
                       instanceId: String,
                       tableId: String)
                      (implicit ev: T <:< Mutation)
    : Future[Tap[(ByteString, Iterable[Mutation])]] = {
      val params = nio.Mutate.Default
      self.write(nio.Mutate[T](projectId, instanceId, tableId))(params)
        .asInstanceOf[Future[Tap[(ByteString, Iterable[Mutation])]]]
    }

    /**
     * Save this SCollection as a Bigtable table. Note that elements must be of type `Mutation`.
     */
    def saveAsBigtable(bigtableOptions: BigtableOptions,
                       tableId: String)
                      (implicit ev: T <:< Mutation)
    : Future[Tap[(ByteString, Iterable[Mutation])]] = {
      val params = nio.Mutate.Default
      self.write(nio.Mutate[T](bigtableOptions, tableId))(params)
        .asInstanceOf[Future[Tap[(ByteString, Iterable[Mutation])]]]
    }

    /**
     * Save this SCollection as a Bigtable table. This version supports batching. Note that
     * elements must be of type `Mutation`.
     */
    def saveAsBigtable(bigtableOptions: BigtableOptions,
                       tableId: String,
                       numOfShards: Int,
                       flushInterval: Duration = Duration.standardSeconds(1))
                      (implicit ev: T <:< Mutation)
    : Future[Tap[(ByteString, Iterable[Mutation])]] = {
      val params = nio.Mutate.Bulk(numOfShards, flushInterval)
      self.write(nio.Mutate[T](bigtableOptions, tableId))(params)
        .asInstanceOf[Future[Tap[(ByteString, Iterable[Mutation])]]]
    }
  }

  case class BigtableInput(projectId: String, instanceId: String, tableId: String)
    extends TestIO[Row](s"$projectId\t$instanceId\t$tableId")

  case class BigtableOutput[T <: Mutation](projectId: String, instanceId: String, tableId: String)
    extends TestIO[(ByteString, Iterable[T])](s"$projectId\t$instanceId\t$tableId")

}
