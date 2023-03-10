/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.bigquery.syntax

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.{BigQueryTypedTable, Table, TableRow, TableRowJsonIO}
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.{SCollection, SCollectionImpl}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.apache.beam.sdk.io.gcp.bigquery.mockUtils.mockedWriteResult
import org.apache.beam.sdk.io.{Compression, TextIO}
import org.apache.beam.sdk.values.{PCollection, PDone}
import org.mockito.scalatest.MockitoSugar

class SCollectionSyntaxTest extends PipelineSpec with MockitoSugar {
  "SCollectionTableRowOps" should "provide PTransform override on saveAsBigQueryTable" in {
    // mock main data SCollection
    val (context, sCollSpy, _) = mockSCollection[TableRow]()
    doReturn(sCollSpy).when(sCollSpy).covary[TableRow]

    // mock error data SCollection
    val (_, failuresSColl, failurePColl) = mockSCollection[TableRow](context)
    when(context.wrap(failurePColl)).thenReturn(failuresSColl)

    // mock BQ write transform
    doReturn(mockedWriteResult(failedInserts = failurePColl))
      .when(sCollSpy)
      .applyInternal(any[Write[TableRow]])

    // mocking away: com.spotify.scio.bigquery.Writes.WriteParamDefauls.defaultInsertErrorTransform
    doReturn(failuresSColl).when(failuresSColl).withName("DropFailedInserts")
    doReturn(null).when(failuresSColl).map(any[TableRow => Unit])(any[Coder[Unit]])

    // create bqIO injecting a mocked beam PTransform
    val beamWrite = mock[Write[TableRow]]
    when(beamWrite.to(any[TableReference])).thenReturn(beamWrite)
    val table = Table.Spec("project:dataset.dummy")
    val bqIO = BigQueryTypedTable(null, beamWrite, table, null)

    /** test * */
    implicit def bigQuerySCollectionTableRowOps[T <: TableRow](sc: SCollection[T]) =
      new SCollectionTableRowOps[T](sc, Option(bqIO))

    sCollSpy.saveAsBigQueryTable(
      table = table,
      configOverride = _.withTableDescription("table-description")
    )

    /** verify * */
    verify(beamWrite).withTableDescription("table-description")

    // spy related
    verify(sCollSpy, atLeast(1)).context
    verify(sCollSpy).write(any[BigQueryTypedTable[TableRow]])(
      any[BigQueryTypedTable.WriteParam[TableRow]]
    )
  }

  "SCollectionTableRowOps" should "provide PTransform override on saveAsTableRowJsonFile" in {
    val textIOWrite = mock[TextIO.Write]

    // mock/spy
    val (_, sCollSpy, _) = mockSCollection[TableRow]()
    doReturn(sCollSpy).when(sCollSpy).covary[TableRow]

    doReturn(null).when(sCollSpy).transform_(any[String])(any[SCollection[TableRow] => PDone])
    doReturn(textIOWrite).when(sCollSpy).textOut(any[String], any[String], anyInt, any[Compression])

    /** test * */
    implicit def bigQuerySCollectionTableRowOps[T <: TableRow](sc: SCollection[T]) =
      new SCollectionTableRowOps[T](sc)

    sCollSpy.saveAsTableRowJsonFile("output-path", configOverride = _.withFooter("fooooter"))

    /** verify * */
    verify(textIOWrite).withFooter("fooooter")

    // spy related
    verify(sCollSpy, atLeast(1)).context
    verify(sCollSpy).write(any[TableRowJsonIO])(any[TableRowJsonIO.WriteParam])
  }

  private def mockSCollection[T](
    contextMock: ScioContext = mock[ScioContext]
  ): (ScioContext, SCollection[T], PCollection[T]) = {
    when(contextMock.isTest).thenReturn(false)
    val pCollMock = mock[PCollection[T]]
    val sCollSpy: SCollection[T] = spy(new SCollectionImpl(pCollMock, contextMock))
    (contextMock, sCollSpy, pCollMock)
  }
}
