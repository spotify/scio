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
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT, TestIO}
import com.spotify.scio.util.Functions
import com.spotify.scio.values.SCollection
import magnolify.bigtable.BigtableType
import org.apache.beam.sdk.io.gcp.{bigtable => beam}
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.typelevel.scalaccompat.annotation.nowarn

import scala.jdk.CollectionConverters._
import scala.util.chaining._

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
    val coder = CoderMaterializer.beam(sc, Coder.protoMessageCoder[Row])
    val read = BigtableRead.read(
      bigtableOptions,
      tableId,
      params.maxBufferElementCount,
      params.keyRanges,
      params.rowFilter
    )
    sc.applyTransform(read).setCoder(coder)
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
    val DefaultKeyRanges: Seq[ByteKeyRange] = Seq.empty[ByteKeyRange]
    val DefaultRowFilter: RowFilter = null
    val DefaultMaxBufferElementCount: Option[Int] = None

    def apply(keyRange: ByteKeyRange) = new ReadParam(Seq(keyRange))

    def apply(keyRange: ByteKeyRange, rowFilter: RowFilter): ReadParam =
      new ReadParam(Seq(keyRange), rowFilter)
  }

  final case class ReadParam private (
    keyRanges: Seq[ByteKeyRange] = ReadParam.DefaultKeyRanges,
    rowFilter: RowFilter = ReadParam.DefaultRowFilter,
    maxBufferElementCount: Option[Int] = ReadParam.DefaultMaxBufferElementCount
  )

  private[scio] def read(
    bigtableOptions: BigtableOptions,
    tableId: String,
    maxBufferElementCount: Option[Int],
    keyRanges: Seq[ByteKeyRange],
    rowFilter: RowFilter
  ): beam.BigtableIO.Read = {
    val opts = bigtableOptions // defeat closure
    beam.BigtableIO
      .read()
      .withProjectId(bigtableOptions.getProjectId)
      .withInstanceId(bigtableOptions.getInstanceId)
      .withTableId(tableId)
      .withBigtableOptionsConfigurator(Functions.serializableFn(_ => opts.toBuilder))
      .withMaxBufferElementCount(maxBufferElementCount.map(Int.box).orNull)
      .pipe(r => if (keyRanges.isEmpty) r else r.withKeyRanges(keyRanges.asJava))
      .pipe(r => Option(rowFilter).fold(r)(r.withRowFilter)): @nowarn("cat=deprecation")
  }
}

final case class BigtableTypedIO[K: Coder, T: BigtableType: Coder](
  bigtableOptions: BigtableOptions,
  tableId: String
) extends BigtableIO[(K, T)] {
  override type ReadP = BigtableTypedIO.ReadParam[K]
  override type WriteP = BigtableTypedIO.WriteParam[K]

  override def testId: String =
    s"BigtableIO(${bigtableOptions.getProjectId}\t${bigtableOptions.getInstanceId}\t$tableId)"

  override protected def read(
    sc: ScioContext,
    params: ReadP
  ): SCollection[(K, T)] = {
    val coder = CoderMaterializer.beam(sc, Coder.protoMessageCoder[Row])
    val read = BigtableRead.read(
      bigtableOptions,
      tableId,
      params.maxBufferElementCount,
      params.keyRanges,
      params.rowFilter
    )

    val bigtableType: BigtableType[T] = implicitly
    val cf = params.columnFamily
    val keyFn = params.keyFn
    sc.transform(
      _.applyTransform(read)
        .setCoder(coder)
        .map(row => keyFn(row.getKey) -> bigtableType(row, cf))
    )
  }

  override protected def write(
    data: SCollection[(K, T)],
    params: WriteP
  ): Tap[Nothing] = {
    val bigtableType: BigtableType[T] = implicitly
    val btParams = params match {
      case d: BigtableTypedIO.Default[_] =>
        BigtableWrite.Default(d.flowControlEnabled)
      case b: BigtableTypedIO.Bulk[_] =>
        BigtableWrite.Bulk(
          b.numOfShards,
          Option(b.flushInterval).getOrElse(BigtableWrite.Bulk.DefaultFlushInterval)
        )
    }
    val cf = params.columnFamily
    val ts = params.timestamp
    val keyFn = params.keyFn
    data.transform_("Bigtable write") { coll =>
      coll
        .map { case (key, t) =>
          val mutations = Iterable(bigtableType.apply(t, cf, ts)).asJava
            .asInstanceOf[java.lang.Iterable[Mutation]]
          KV.of(keyFn(key), mutations)
        }
        .applyInternal(BigtableWrite.sink(tableId, bigtableOptions, btParams))
    }
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    throw new NotImplementedError("Bigtable tap not implemented")
}

object BigtableTypedIO {
  object ReadParam {
    val DefaultKeyRanges: Seq[ByteKeyRange] = Seq.empty[ByteKeyRange]
    val DefaultRowFilter: RowFilter = null
    val DefaultMaxBufferElementCount: Option[Int] = None
  }

  final case class ReadParam[K] private (
    columnFamily: String,
    keyFn: ByteString => K,
    keyRanges: Seq[ByteKeyRange] = ReadParam.DefaultKeyRanges,
    rowFilter: RowFilter = ReadParam.DefaultRowFilter,
    maxBufferElementCount: Option[Int] = ReadParam.DefaultMaxBufferElementCount
  )

  sealed trait WriteParam[K] {
    val columnFamily: String
    val keyFn: K => ByteString
    val timestamp: Long
  }
  object WriteParam {
    val DefaultTimestamp: Long = 0L
    val DefaultNumOfShards: Option[Int] = None
    val DefaultFlushInterval: Duration = null
    val DefaultFlowControlEnabled = false
  }

  final case class Default[K] private (
    columnFamily: String,
    keyFn: K => ByteString,
    timestamp: Long = WriteParam.DefaultTimestamp,
    flowControlEnabled: Boolean = WriteParam.DefaultFlowControlEnabled
  ) extends WriteParam[K]

  final case class Bulk[K] private (
    columnFamily: String,
    keyFn: K => ByteString,
    timestamp: Long,
    numOfShards: Int,
    flushInterval: Duration
  ) extends WriteParam[K]
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
    data.transform_("Bigtable write") { coll =>
      coll
        .map { case (key, value) =>
          KV.of(key, value.asJava.asInstanceOf[java.lang.Iterable[Mutation]])
        }
        .applyInternal(BigtableWrite.sink(tableId, bigtableOptions, params))
    }
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object BigtableWrite {
  sealed trait WriteParam

  object Default {
    private[bigtable] val DefaultFlowControlEnabled = false
  }
  final case class Default(flowControlEnabled: Boolean = Default.DefaultFlowControlEnabled)
      extends WriteParam

  object Bulk {
    private[bigtable] val DefaultFlushInterval = Duration.standardSeconds(1)
  }

  final case class Bulk private (
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

  private[scio] def sink(tableId: String, bigtableOptions: BigtableOptions, params: WriteParam) = {
    params match {
      case BigtableWrite.Default(flowControlEnabled) =>
        val opts = bigtableOptions // defeat closure
        beam.BigtableIO
          .write()
          .withProjectId(bigtableOptions.getProjectId)
          .withInstanceId(bigtableOptions.getInstanceId)
          .withTableId(tableId)
          .withFlowControl(flowControlEnabled)
          .withBigtableOptionsConfigurator(
            Functions.serializableFn(_ => opts.toBuilder)
          ): @nowarn("cat=deprecation")
      case BigtableWrite.Bulk(numOfShards, flushInterval) =>
        new BigtableBulkWriter(tableId, bigtableOptions, numOfShards, flushInterval)
    }
  }
}
