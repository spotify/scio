/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.bigtable

import com.google.bigtable.v2._
import com.google.cloud.bigtable.config.BigtableOptions
import com.google.protobuf.ByteString
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TestIO}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.{bigtable => beam}
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration

import scala.jdk.CollectionConverters._
import com.spotify.scio.io.TapT
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone

sealed trait BigtableIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]
}

object BigtableIO {
  final def apply[T](projectId: String, instanceId: String, tableId: String): BigtableIO[T] =
    new BigtableIO[T] with TestIO[T] {
      override def testId: String =
        s"BigtableIO($projectId\t$instanceId\t$tableId)"
    }
}

final case class BigtableRead(bigtableOptions: BigtableOptions, tableId: String)
    extends BigtableIO[Row] {
  override type ReadP = BigtableRead.ReadParam
  override type WriteP = Nothing

  override def testId: String =
    s"BigtableIO(${bigtableOptions.getProjectId}\t${bigtableOptions.getInstanceId}\t$tableId)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Row] = {
    val opts = bigtableOptions // defeat closure
    var read = beam.BigtableIO
      .read()
      .withProjectId(bigtableOptions.getProjectId)
      .withInstanceId(bigtableOptions.getInstanceId)
      .withTableId(tableId)
      .withBigtableOptionsConfigurator(
        new SerializableFunction[BigtableOptions.Builder, BigtableOptions.Builder] {
          override def apply(input: BigtableOptions.Builder): BigtableOptions.Builder =
            opts.toBuilder
        }
      )
    if (!params.keyRanges.isEmpty) {
      read = read.withKeyRanges(params.keyRanges.asJava)
    }
    if (params.rowFilter != null) {
      read = read.withRowFilter(params.rowFilter)
    }
    sc.applyTransform(read)
  }

  override protected def write(data: SCollection[Row], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException(
      "BigtableRead is read-only, use Mutation to write to Bigtable"
    )

  override def tap(params: ReadP): Tap[Nothing] =
    throw new NotImplementedError("Bigtable tap not implemented")
}

object BigtableRead {
  object ReadParam {
    private[bigtable] val DefaultKeyRanges: Seq[ByteKeyRange] = Seq.empty[ByteKeyRange]
    private[bigtable] val DefaultRowFilter: RowFilter = null

    def apply(keyRange: ByteKeyRange) = new ReadParam(Seq(keyRange))

    def apply(keyRange: ByteKeyRange, rowFilter: RowFilter): ReadParam =
      new ReadParam(Seq(keyRange), rowFilter)
  }

  final case class ReadParam private[bigtable] (
    keyRanges: Seq[ByteKeyRange] = ReadParam.DefaultKeyRanges,
    rowFilter: RowFilter = ReadParam.DefaultRowFilter
  )

  final def apply(projectId: String, instanceId: String, tableId: String): BigtableRead = {
    val bigtableOptions = BigtableOptions
      .builder()
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .build
    BigtableRead(bigtableOptions, tableId)
  }
}

final case class BigtableWrite[T <: Mutation](bigtableOptions: BigtableOptions, tableId: String)
    extends BigtableIO[(ByteString, Iterable[T])] {
  override type ReadP = Nothing
  override type WriteP = BigtableWrite.WriteParam

  override def testId: String =
    s"BigtableIO(${bigtableOptions.getProjectId}\t${bigtableOptions.getInstanceId}\t$tableId)"

  override protected def read(
    sc: ScioContext,
    params: ReadP
  ): SCollection[(ByteString, Iterable[T])] =
    throw new UnsupportedOperationException(
      "BigtableWrite is write-only, use Row to read from Bigtable"
    )

  override protected def write(
    data: SCollection[(ByteString, Iterable[T])],
    params: WriteP
  ): Tap[Nothing] = {
    val sink =
      params match {
        case BigtableWrite.Default =>
          val opts = bigtableOptions // defeat closure
          beam.BigtableIO
            .write()
            .withProjectId(bigtableOptions.getProjectId)
            .withInstanceId(bigtableOptions.getInstanceId)
            .withTableId(tableId)
            .withBigtableOptionsConfigurator(
              new SerializableFunction[BigtableOptions.Builder, BigtableOptions.Builder] {
                override def apply(input: BigtableOptions.Builder): BigtableOptions.Builder =
                  opts.toBuilder
              }
            )
        case BigtableWrite.Bulk(numOfShards, flushInterval) =>
          new BigtableBulkWriter(tableId, bigtableOptions, numOfShards, flushInterval)
      }
    data.transform_("Bigtable write") { coll =>
      coll
        .map { case (key, value) =>
          KV.of(key, value.asJava.asInstanceOf[java.lang.Iterable[Mutation]])
        }
        .applyInternal[PDone](sink.asInstanceOf[PTransform[PCollection[KV[Serializable, java.lang.Iterable[Mutation]]], PDone]])
    }
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object BigtableWrite {
  sealed trait WriteParam
  object Default extends WriteParam

  object Bulk {
    private[bigtable] val DefaultFlushInterval = Duration.standardSeconds(1)
  }

  final case class Bulk private[bigtable] (
    numOfShards: Int,
    flushInterval: Duration = Bulk.DefaultFlushInterval
  ) extends WriteParam

  final def apply[T <: Mutation](
    projectId: String,
    instanceId: String,
    tableId: String
  ): BigtableWrite[T] = {
    val bigtableOptions = BigtableOptions
      .builder()
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .build
    BigtableWrite[T](bigtableOptions, tableId)
  }
}
