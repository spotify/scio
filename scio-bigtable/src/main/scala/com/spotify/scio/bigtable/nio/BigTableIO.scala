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

import com.google.bigtable.v2.{Row => BTRow, _}
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.protobuf.ByteString
import com.spotify.scio.ScioContext
import com.spotify.scio.bigtable._
import com.spotify.scio.io.Tap
import com.spotify.scio.nio.{ScioIO, TestIO}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigtable.{BigtableIO => BBigtableIO}
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration

import scala.collection.JavaConverters._
import scala.concurrent.Future

sealed trait BigtableIO[T] extends ScioIO[T]

object BigtableIO {
  def apply[T](projectId: String,
               instanceId: String,
               tableId: String): BigtableIO[T] = new BigtableIO[T] with TestIO[T] {
    override def testId: String = s"BigtableIO($projectId\t$instanceId\t$tableId)"
  }
}

final case class Row(bigtableOptions: BigtableOptions, tableId: String) extends BigtableIO[BTRow] {

  override type ReadP = Row.ReadParam
  override type WriteP = Nothing

  override def testId: String =
    s"BigtableIO(${bigtableOptions.getProjectId}\t${bigtableOptions.getInstanceId}\t$tableId)"

  override def read(sc: ScioContext, params: ReadP): SCollection[BTRow] = {
    val opts = bigtableOptions // defeat closure
    var read = BBigtableIO.read()
      .withProjectId(bigtableOptions.getProjectId)
      .withInstanceId(bigtableOptions.getInstanceId)
      .withTableId(tableId)
      .withBigtableOptionsConfigurator(
        new SerializableFunction[BigtableOptions.Builder, BigtableOptions.Builder] {
          override def apply(input: BigtableOptions.Builder): BigtableOptions.Builder =
            opts.toBuilder
        })
    if (params.keyRange != null) {
      read = read.withKeyRange(params.keyRange)
    }
    if (params.rowFilter != null) {
      read = read.withRowFilter(params.rowFilter)
    }
    sc.wrap(sc.applyInternal(read))
      .setName(s"${bigtableOptions.getProjectId} ${bigtableOptions.getInstanceId} $tableId")
  }

  override def write(data: SCollection[BTRow], params: WriteP): Future[Tap[BTRow]] =
    throw new IllegalStateException("Row is read-only, use Mutation to write to Bigtable")

  override def tap(params: ReadP): Tap[BTRow] =
    throw new NotImplementedError("Bigtable tap not implemented")
}

object Row {
  final case class ReadParam(
    keyRange: ByteKeyRange = null,
    rowFilter: RowFilter = null)

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

final case class Mutate[T](bigtableOptions: BigtableOptions,
                           tableId: String)
                          (implicit ev: T <:< Mutation)
  extends BigtableIO[(ByteString, Iterable[T])] {

  override type ReadP = Nothing
  override type WriteP = Mutate.WriteParam

  override def testId: String =
    s"BigtableIO(${bigtableOptions.getProjectId}\t${bigtableOptions.getInstanceId}\t$tableId)"

  override def read(sc: ScioContext, params: ReadP): SCollection[(ByteString, Iterable[T])] =
    throw new IllegalStateException("Mutate is write-only, use Row to read from Bigtable")

  override def write(data: SCollection[(ByteString, Iterable[T])], params: WriteP)
  : Future[Tap[(ByteString, Iterable[T])]] = {
    val sink =
      params match {
        case Mutate.Default =>
          val opts = bigtableOptions // defeat closure
          BBigtableIO.write()
            .withProjectId(bigtableOptions.getProjectId)
            .withInstanceId(bigtableOptions.getInstanceId)
            .withTableId(tableId)
            .withBigtableOptionsConfigurator(
              new SerializableFunction[BigtableOptions.Builder, BigtableOptions.Builder] {
                override def apply(input: BigtableOptions.Builder): BigtableOptions.Builder =
                  opts.toBuilder
              })
        case Mutate.Bulk(numOfShards, flushInterval) =>
          new BigtableBulkWriter(tableId, bigtableOptions, numOfShards, flushInterval)
      }
    data
      .map(kv => KV.of(kv._1, kv._2.asJava.asInstanceOf[java.lang.Iterable[Mutation]]))
      .applyInternal(sink)
    Future.failed(new NotImplementedError("Bigtable future not implemented"))
  }

  override def tap(params: ReadP): Tap[(ByteString, Iterable[T])] =
    throw new NotImplementedError("Bigtable tap not implemented")
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
