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

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.{BigQueryType, BigQueryTypedTable, Table, TableRow}
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.{SCollection, SCollectionImpl}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.apache.beam.sdk.io.gcp.bigquery.mockUtils.mockedWriteResult
import org.apache.beam.sdk.io.{Compression, TextIO}
import org.apache.beam.sdk.values.{PCollection, PDone}
import org.mockito.Mockito.lenient
import org.mockito.scalatest.MockitoSugar

class SCollectionSyntaxTest extends PipelineSpec with MockitoSugar {

  "SCollectionTableRowOps" should "provide PTransform override on saveAsBigQueryTable" in {
    // mock main data SCollection
    val (_, sCollSpy, _) = mockPipe[TableRow]()

    // create bqIO injecting a mocked beam PTransform
    val (table, bqIO, beamWrite) = mockTypedWriteIO[TableRow]()

    /** test */
    implicit def bigQuerySCollectionTableRowOps[T <: TableRow](sc: SCollection[T]) =
      new SCollectionTableRowOps[T](sc, Option(bqIO))

    sCollSpy.saveAsBigQueryTable(
      table = table,
      configOverride = _.withTableDescription("table-description")
    )

    /** verify */
    verify(beamWrite).withTableDescription("table-description")

    /** after */
    expectedBySpy(sCollSpy)
  }

  "SCollectionTableRowOps" should "provide PTransform override on saveAsTableRowJsonFile" in {
    val textIOWrite = mock[TextIO.Write]

    // mock/spy
    val (_, sCollSpy, _) = mockPipe[TableRow]()

    doReturn(null).when(sCollSpy).transform_(any[String])(any[SCollection[TableRow] => PDone])
    doReturn(textIOWrite).when(sCollSpy).textOut(any[String], any[String], anyInt, any[Compression])

    /** test */
    implicit def bigQuerySCollectionTableRowOps[T <: TableRow](sc: SCollection[T]) =
      new SCollectionTableRowOps[T](sc)

    sCollSpy.saveAsTableRowJsonFile("output-path", configOverride = _.withFooter("fooooter"))

    /** verify */
    verify(textIOWrite).withFooter("fooooter")

    /** after */
    expectedBySpy(sCollSpy)
  }

  "SCollectionGenericRecordOps" should "provide PTransform override on saveAsBigQueryTable" in {
    // mock main data SCollection
    val (_, sCollSpy, _) = mockPipe[GenericRecord]()

    // create bqIO injecting a mocked beam PTransform
    val (table, bqIO, beamWrite) = mockTypedWriteIO[GenericRecord]()

    /** test */
    implicit def bigQuerySCollectionGenericRecordOps[T <: GenericRecord](
      sc: SCollection[T]
    ): SCollectionGenericRecordOps[T] =
      new SCollectionGenericRecordOps[T](sc, Some(bqIO))

    sCollSpy.saveAsBigQueryTable(
      table = table,
      configOverride = _.withTableDescription("table-description")
    )

    /** verify */
    verify(beamWrite).withTableDescription("table-description")

    /** after */
    expectedBySpy(sCollSpy)
  }

  "SCollectionTypedOps" should "provide PTransform override on saveAsTypedBigQueryTable" in {
    import SCollectionSyntaxTest.BQRecord

    // mock/spy
    val (_, sCollSpy, _) = mockPipe[BQRecord]()
    doReturn("some-tfname").when(sCollSpy).tfName

    // create bqIO injecting a mocked beam PTransform
    val (table, bqIO, beamWrite) = mockTypedWriteIO[BQRecord]()
    when(beamWrite.withSchema(any[TableSchema])).thenReturn(beamWrite)

    /** test */
    implicit def bigQuerySCollectionTypedOps(
      sc: SCollection[BQRecord]
    ): SCollectionTypedOps[BQRecord] =
      new SCollectionTypedOps[BQRecord](sc, Option(bqIO))

    sCollSpy.saveAsTypedBigQueryTable(table, configOverride = _.withKmsKey("SomeKmsKey"))

    /** verify */
    verify(beamWrite).withKmsKey("SomeKmsKey")

    /** after */
    expectedBySpy(sCollSpy)
  }

  private def mockTypedWriteIO[T: Coder](): (Table.Spec, BigQueryTypedTable[T], Write[T]) = {
    val table = Table.Spec("project:dataset.dummy")
    val beamWrite = mock[Write[T]]
    when(beamWrite.to(any[TableReference])).thenReturn(beamWrite)
    val bqIO = BigQueryTypedTable(null, beamWrite, table, null)
    (table, bqIO, beamWrite)
  }

  private def mockPipe[T](
    context: ScioContext = mock[ScioContext]
  ): (ScioContext, SCollection[T], PCollection[T]) = {
    when(context.isTest).thenReturn(false)
    val (sCollSpy, pCollMock) = stubSCollection[T](context)
    val (failuresSColl, failurePColl) = stubSCollection[TableRow](context)

    // mocking away: com.spotify.scio.bigquery.Writes.WriteParamDefauls.defaultInsertErrorTransform
    lenient().doReturn(failuresSColl).when(failuresSColl).withName("DropFailedInserts")
    lenient().doReturn(null).when(failuresSColl).map(any[TableRow => Unit])(any[Coder[Unit]])

    // mock BQ write transform
    lenient()
      .doReturn(mockedWriteResult(failedInserts = failurePColl))
      .when(sCollSpy)
      .applyInternal(any[Write[T]])

    (context, sCollSpy, pCollMock)
  }

  private def stubSCollection[T](context: ScioContext): (SCollection[T], PCollection[T]) = {
    val pColl = mock[PCollection[T]]
    val sCollSpy = spy(new SCollectionImpl(pColl, context))

    lenient().when(context.wrap(pColl)).thenReturn(sCollSpy)
    lenient().doReturn(sCollSpy).when(sCollSpy).withName(any[String])
    lenient().when(sCollSpy.context).thenReturn(context)
    lenient().when(sCollSpy.covary[T]).thenReturn(sCollSpy)

    (sCollSpy, pColl)
  }

  // TODO: lenient
  def expectedBySpy[T](sCollSpy: SCollection[T]): Unit = {
    // spy related
    verify(sCollSpy, atLeast(1)).write(any[BigQueryTypedTable[T]])(
      any[BigQueryTypedTable.WriteParam[T]]
    )
  }
}

object SCollectionSyntaxTest {
  @BigQueryType.toTable
  case class BQRecord(i: Int, s: String, r: List[String])
}
