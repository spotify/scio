/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.bigtable.nio

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.io.Tap
import com.spotify.scio.bigtable._
import com.google.bigtable.v2.{Row => BTRow, _}
import com.google.protobuf.ByteString
import com.google.cloud.bigtable.config.BigtableOptions
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.apache.beam.sdk.values.KV
import scala.concurrent.Future
import scala.collection.JavaConverters._
import org.joda.time.Duration

final case class Row(bigtableOptions: BigtableOptions, tableId: String) extends ScioIO[BTRow] {

  type ReadP = Row.ReadParam
  type WriteP = Nothing

  import bigtableOptions._
  def id: String = s"$getProjectId\t$getInstanceId\t$tableId"

  def read(sc: ScioContext, params: ReadP): SCollection[BTRow] =
    sc.requireNotClosed {
      params match {
        case Row.Parameters(keyRange, rowFilter) =>
          var read = BigtableIO.read().withBigtableOptions(bigtableOptions).withTableId(tableId)
            if (keyRange != null) {
              read = read.withKeyRange(keyRange)
            }
            if (rowFilter != null) {
              read = read.withRowFilter(rowFilter)
            }
            sc.wrap(sc.applyInternal(read))
              .setName(s"${bigtableOptions.getProjectId} ${bigtableOptions.getInstanceId} $tableId")
      }
    }

  def tap(read: ReadP): Tap[BTRow] =
    throw new NotImplementedError("Bigtable tap not implemented")

  def write(sc: SCollection[BTRow], params: WriteP): Future[Tap[BTRow]] =
    throw new IllegalStateException("Row are read-only, Use Mutation to write to BigTable")
}

object Row {
  sealed trait ReadParam
  final case class Parameters(
    keyRange: ByteKeyRange = null,
    rowFilter: RowFilter = null) extends ReadParam

  def apply(projectId: String,
            instanceId: String,
            tableId: String): Row = {
      val bigtableOptions = new BigtableOptions.Builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build
      Row(bigtableOptions, tableId)
    }
}

final case class Mutate[T](
  bigtableOptions: BigtableOptions,
  tableId: String)(implicit ev: T <:< Mutation) extends ScioIO[(ByteString, Iterable[T])] {

  type ReadP = Nothing
  type WriteP = Mutate.WriteParam

  import bigtableOptions._
  def id: String = s"$getProjectId\t$getInstanceId\t$tableId"

  def read(sc: ScioContext, params: ReadP): SCollection[(ByteString, Iterable[T])] =
    throw new IllegalStateException("Can't read mutations from BigTable")

  def tap(read: ReadP): Tap[(ByteString, Iterable[T])] =
    throw new IllegalStateException("Can't read mutations from BigTable")

  def write(
    sc: SCollection[(ByteString, Iterable[T])],
    params: WriteP
  ): Future[Tap[(ByteString, Iterable[T])]] = {
    val sink =
      params match {
        case Mutate.Default =>
          BigtableIO.write().withBigtableOptions(bigtableOptions).withTableId(tableId)
        case Mutate.Bulk(numOfShards, flushInterval) =>
          new BigtableBulkWriter(tableId, bigtableOptions, numOfShards, flushInterval)
      }
    sc.map(kv => KV.of(kv._1, kv._2.asJava.asInstanceOf[java.lang.Iterable[Mutation]]))
      .applyInternal(sink)
    Future.failed(new NotImplementedError("Bigtable future not implemented"))
  }
}

object Mutate {
  sealed trait WriteParam
  object Default extends WriteParam
  final case class Bulk(
    numOfShards: Int,
    flushInterval: Duration = Duration.standardSeconds(1)) extends WriteParam

  def apply[T](
            projectId: String,
            instanceId: String,
            tableId: String)(implicit ev: T <:< Mutation): Mutate[T] = {
      val bigtableOptions = new BigtableOptions.Builder()
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build
      Mutate[T](bigtableOptions, tableId)
    }
}
