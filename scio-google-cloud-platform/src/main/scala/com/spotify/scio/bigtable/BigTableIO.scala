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

import com.google.cloud.bigtable.beam.{
  CloudBigtableIO,
  CloudBigtableScanConfiguration,
  CloudBigtableTableConfiguration
}
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.CoderMaterializer
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Read
import org.apache.beam.sdk.io.range.ByteKeyRange
import org.apache.hadoop.hbase.client.{Mutation, Result, Scan}
import org.apache.hadoop.hbase.filter.Filter

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

final case class BigtableRead(config: CloudBigtableTableConfiguration) extends BigtableIO[Result] {
  override type ReadP = BigtableRead.ReadParam
  override type WriteP = Nothing

  override def testId: String =
    s"BigtableIO(${config.getProjectId}\t${config.getInstanceId}\t${config.getTableId})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Result] = {
    val coder = CoderMaterializer.beam(sc, BigtableCoders.resultCoder)
    val scan = new Scan()
      .pipe(s =>
        Option(params.keyRange).fold(s) { kr =>
          s.withStartRow(kr.getStartKey.getBytes).withStopRow(kr.getEndKey.getBytes)
        }
      )
      .pipe(s => Option(params.filter).fold(s)(s.setFilter))

    val scanConfig = CloudBigtableScanConfiguration.fromConfig(config, scan)
    val read = Read.from(CloudBigtableIO.read(scanConfig))

    sc.applyTransform(read).setCoder(coder)
  }

  override protected def write(data: SCollection[Result], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException(
      "BigtableRead is read-only, use Mutation to write to Bigtable"
    )

  override def tap(params: ReadP): Tap[Nothing] =
    throw new NotImplementedError("Bigtable tap not implemented")
}

object BigtableRead {
  object ReadParam {
    val DefaultKeyRange: ByteKeyRange = null
    val DefaultFilter: Filter = null
  }

  final case class ReadParam private (
    keyRange: ByteKeyRange = ReadParam.DefaultKeyRange,
    filter: Filter = ReadParam.DefaultFilter
  )

  final def apply(projectId: String, instanceId: String, tableId: String): BigtableRead = {
    val config = new CloudBigtableTableConfiguration.Builder()
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .withTableId(tableId)
      .build
    BigtableRead(config)
  }
}

final case class BigtableWrite[T <: Mutation](config: CloudBigtableTableConfiguration)
    extends BigtableIO[T] {
  override type ReadP = Nothing
  override type WriteP = Unit

  override def testId: String =
    s"BigtableIO(${config.getProjectId}\t${config.getInstanceId}\t${config.getTableId})"

  override protected def read(
    sc: ScioContext,
    params: ReadP
  ): SCollection[T] =
    throw new UnsupportedOperationException(
      "BigtableWrite is write-only, use Row to read from Bigtable"
    )

  override protected def write(
    data: SCollection[T],
    params: WriteP
  ): Tap[Nothing] = {
    CloudBigtableIO.writeToTable(config)

    data
      .covary[Mutation]
      .applyInternal("Bigtable write", CloudBigtableIO.writeToTable(config))
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object BigtableWrite {

  final def apply[T <: Mutation](
    projectId: String,
    instanceId: String,
    tableId: String
  ): BigtableWrite[T] = {
    val config = new CloudBigtableTableConfiguration.Builder()
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .withTableId(tableId)
      .build
    BigtableWrite[T](config)
  }
}
