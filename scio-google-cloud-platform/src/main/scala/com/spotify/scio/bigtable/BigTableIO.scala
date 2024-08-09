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
import com.google.protobuf.ByteString
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT, TestIO, WriteResultIO}
import com.spotify.scio.values.{SCollection, SideOutput, SideOutputCollections}
import org.apache.beam.sdk.io.gcp.bigtable.{BigtableWriteResult, BigtableWriteResultCoder}
import org.apache.beam.sdk.io.gcp.{bigtable => beam}
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.apache.beam.sdk.transforms.errorhandling.{BadRecord, ErrorHandler}
import org.apache.beam.sdk.values.{KV, PCollectionTuple}
import org.joda.time.Duration

import scala.jdk.CollectionConverters._
import scala.util.chaining._

sealed abstract class BigtableIO[T](projectId: String, instanceId: String, tableId: String)
    extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]
  override def testId: String = s"BigtableIO($projectId:$instanceId:$tableId)"
}

object BigtableIO {
  final def apply[T](projectId: String, instanceId: String, tableId: String): BigtableIO[T] =
    new BigtableIO[T](projectId, instanceId, tableId) with TestIO[T]
}

final case class BigtableRead(projectId: String, instanceId: String, tableId: String)
    extends BigtableIO[Row](projectId, instanceId, tableId) {
  override type ReadP = BigtableRead.ReadParam
  override type WriteP = Nothing

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Row] = {
    val coder = CoderMaterializer.beam(sc, Coder.protoMessageCoder[Row])
    val read = beam.BigtableIO
      .read()
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .withTableId(tableId)
      .withKeyRanges(params.keyRanges.asJava)
      .pipe(r => Option(params.rowFilter).fold(r)(r.withRowFilter))
      .pipe(r => params.maxBufferElementCount.fold(r)(r.withMaxBufferElementCount(_)))
      .pipe(r => Option(params.appProfileId).fold(r)(r.withAppProfileId))
      .pipe(r => Option(params.attemptTimeout).fold(r)(r.withAttemptTimeout))
      .pipe(r => Option(params.operationTimeout).fold(r)(r.withOperationTimeout))

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
    val DefaultKeyRanges: Seq[ByteKeyRange] = Seq(ByteKeyRange.ALL_KEYS)
    val DefaultRowFilter: RowFilter = null
    val DefaultMaxBufferElementCount: Option[Int] = None
    val DefaultAppProfileId: String = null
    val DefaultAttemptTimeout: Duration = null
    val DefaultOperationTimeout: Duration = null
  }

  final case class ReadParam private (
    keyRanges: Seq[ByteKeyRange] = ReadParam.DefaultKeyRanges,
    rowFilter: RowFilter = ReadParam.DefaultRowFilter,
    maxBufferElementCount: Option[Int] = ReadParam.DefaultMaxBufferElementCount,
    appProfileId: String = ReadParam.DefaultAppProfileId,
    attemptTimeout: Duration = ReadParam.DefaultAttemptTimeout,
    operationTimeout: Duration = ReadParam.DefaultOperationTimeout
  )
}

final case class BigtableWrite[T <: Mutation](
  projectId: String,
  instanceId: String,
  tableId: String
) extends BigtableIO[(ByteString, Iterable[T])](projectId, instanceId, tableId)
    with WriteResultIO[(ByteString, Iterable[T])] {
  override type ReadP = Unit
  override type WriteP = BigtableWrite.WriteParam

  override protected def read(
    sc: ScioContext,
    params: ReadP
  ): SCollection[(ByteString, Iterable[T])] =
    throw new UnsupportedOperationException(
      "BigtableWrite is write-only, use Row to read from Bigtable"
    )

  override protected def writeWithResult(
    data: SCollection[(ByteString, Iterable[T])],
    params: WriteP
  ): (Tap[Nothing], SideOutputCollections) = {
    val t = beam.BigtableIO
      .write()
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .withTableId(tableId)
      .withFlowControl(params.flowControl)
      .pipe(w => Option(params.errorHandler).fold(w)(w.withErrorHandler))
      .pipe(w => Option(params.appProfileId).fold(w)(w.withAppProfileId))
      .pipe(w => Option(params.attemptTimeout).fold(w)(w.withAttemptTimeout))
      .pipe(w => Option(params.operationTimeout).fold(w)(w.withOperationTimeout))
      .pipe(w => params.maxBytesPerBatch.fold(w)(w.withMaxBytesPerBatch))
      .pipe(w => params.maxElementsPerBatch.fold(w)(w.withMaxElementsPerBatch))
      .pipe(w => params.maxOutstandingBytes.fold(w)(w.withMaxOutstandingBytes))
      .pipe(w => params.maxOutstandingElements.fold(w)(w.withMaxOutstandingElements))
      .withWriteResults()

    val result = data.transform_("Bigtable write") { coll =>
      coll
        .map { case (key, mutations) => KV.of(key, (mutations: Iterable[Mutation]).asJava) }
        .applyInternal(t)
    }
    val sideOutput = PCollectionTuple.of(BigtableWrite.BigtableWriteResult.tupleTag, result)
    (tap(()), SideOutputCollections(sideOutput, data.context))
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object BigtableWrite {

  // TODO should this be here ?
  implicit val bigtableWriteResultCoder: Coder[BigtableWriteResult] =
    Coder.beam(new BigtableWriteResultCoder)

  lazy val BigtableWriteResult: SideOutput[BigtableWriteResult] = SideOutput()

  object WriteParam {
    val DefaultFlowControl: Boolean = false
    val DefaultErrorHandler: ErrorHandler[BadRecord, _] = null
    val DefaultAppProfileId: String = null
    val DefaultAttemptTimeout: Duration = null
    val DefaultOperationTimeout: Duration = null
    val DefaultMaxBytesPerBatch: Option[Long] = None
    val DefaultMaxElementsPerBatch: Option[Long] = None
    val DefaultMaxOutstandingBytes: Option[Long] = None
    val DefaultMaxOutstandingElements: Option[Long] = None
  }

  final case class WriteParam private (
    flowControl: Boolean = WriteParam.DefaultFlowControl,
    errorHandler: ErrorHandler[BadRecord, _] = WriteParam.DefaultErrorHandler,
    appProfileId: String = WriteParam.DefaultAppProfileId,
    attemptTimeout: Duration = WriteParam.DefaultAttemptTimeout,
    operationTimeout: Duration = WriteParam.DefaultOperationTimeout,
    maxBytesPerBatch: Option[Long] = WriteParam.DefaultMaxBytesPerBatch,
    maxElementsPerBatch: Option[Long] = WriteParam.DefaultMaxElementsPerBatch,
    maxOutstandingBytes: Option[Long] = WriteParam.DefaultMaxOutstandingBytes,
    maxOutstandingElements: Option[Long] = WriteParam.DefaultMaxOutstandingElements
  )
}
